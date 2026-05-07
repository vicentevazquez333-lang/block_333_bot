"""
Bot de Telegram - Consulta de Cédulas Venezolanas
──────────────────────────────────────────────────
• En LOCAL:      usa polling (sin configuración extra)
• En PRODUCCIÓN: usa webhooks (Render / Koyeb / Railway)

Instalar dependencias:
    pip install -r requirements.txt
"""

import os
import re
import json
import asyncio
from io import BytesIO
import logging
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv

    load_dotenv()
    load_dotenv("VARIABLES_BOT.env.txt")
except ImportError:
    pass

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ════════════════════════════════════════════════════════
#  CONFIGURACIÓN  (se lee de variables de entorno primero,
#  y si no existen, usa los valores escritos aquí abajo)
# ════════════════════════════════════════════════════════
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8617966761:AAEu-DoargseqjW1l74cMuluQJhjaE71D7g")
API_APP_ID     = os.environ.get("API_APP_ID",     "TU_APP_ID_AQUI")
API_TOKEN      = os.environ.get("API_TOKEN",      "TU_API_TOKEN_AQUI")

# Solo para modo webhook (producción en la nube)
# Ejemplo: https://mi-bot.koyeb.app
WEBHOOK_URL    = os.environ.get("WEBHOOK_URL", "")
PORT           = int(os.environ.get("PORT", 8080))
# Lista blanca: IDs numéricos de Telegram separados por coma (solo esos usuarios pueden usar el bot).
# Obtén tu ID hablando con @userinfobot o @RawDataBot. Si está vacío, cualquiera puede usar el bot.
# Ejemplo: TELEGRAM_ALLOWED_USER_IDS=123456789,987654321
# ════════════════════════════════════════════════════════

API_URL  = "https://api.cedula.com.ve/api/v1"

# IVSS - Constancia de Cotizaciones
IVSS_URL = "http://www.ivss.gob.ve:28088/ConstanciaCotizacion/BuscaCotizacionCTRL"

# INTT - Consulta de Vehículos (Laravel Livewire)
INTT_BASE_URL  = "http://consulta.intt.gob.ve"
INTT_LOGIN_URL = f"{INTT_BASE_URL}/ingreso"
INTT_UPDATE_URL = f"{INTT_BASE_URL}/livewire/update"
INTT_VEH_URL   = f"{INTT_BASE_URL}/consulta-vehiculos"
SENIAT_URL     = "http://contribuyente.seniat.gob.ve/relacionesrif/inicioConsulta.jsp"

INTT_USER      = os.environ.get("INTT_USER", "Ee30743649@gmail.com")
INTT_PASS      = os.environ.get("INTT_PASS", "30743649Ee.")

ESPERANDO_CEDULA = 1

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _parse_allowed_telegram_user_ids() -> frozenset[int]:
    """Lee TELEGRAM_ALLOWED_USER_IDS; solo dígitos por segmento (tolerante a espacios/BOM/raros)."""
    raw = os.environ.get("TELEGRAM_ALLOWED_USER_IDS", "").strip()
    raw = raw.lstrip("\ufeff")
    if not raw:
        return frozenset()
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        digits = "".join(c for c in part if c.isdigit())
        if not digits:
            if part:
                logger.warning("TELEGRAM_ALLOWED_USER_IDS: segmento sin dígitos útiles: %r", part)
            continue
        uid = int(digits)
        if uid > 0:
            out.append(uid)
    return frozenset(out)


def _user_id_from_update(update: Update) -> int | None:
    """ID de Telegram del actor (privados, grupos y callbacks)."""
    if update.effective_user is not None:
        return update.effective_user.id
    if update.message and update.message.from_user:
        return update.message.from_user.id
    if update.edited_message and update.edited_message.from_user:
        return update.edited_message.from_user.id
    if update.callback_query and update.callback_query.from_user:
        return update.callback_query.from_user.id
    return None


def user_has_access(update: Update) -> bool:
    """Lee la lista blanca desde el entorno en cada llamada (evita estado desfasado)."""
    allowed = _parse_allowed_telegram_user_ids()
    if not allowed:
        return True
    uid = _user_id_from_update(update)
    if uid is None:
        return False
    return uid in allowed


async def ensure_user_allowed(update: Update) -> bool:
    """Si hay lista blanca configurada, bloquea al resto. Devuelve True si puede continuar."""
    if user_has_access(update):
        return True
    uid = _user_id_from_update(update)
    logger.warning(
        "Acceso denegado: user_id=%s env_TELEGRAM_ALLOWED_USER_IDS=%r",
        uid,
        os.environ.get("TELEGRAM_ALLOWED_USER_IDS"),
    )
    if update.message:
        await update.message.reply_text("🚫 No tienes permiso para usar este bot.")
    elif update.edited_message:
        await update.edited_message.reply_text("🚫 No tienes permiso para usar este bot.")
    elif update.callback_query:
        await update.callback_query.answer("No autorizado.", show_alert=True)
    return False


async def access_denied_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Respuesta única para quien no pasa filters.User en comandos/mensajes."""
    if update.message:
        await update.message.reply_text("🚫 No tienes permiso para usar este bot.")


# ─────────────────────────────────────────────
#  Consulta a la API de cedula.com.ve
# ─────────────────────────────────────────────
_CEDULA_API_ERR_MSG = {
    "INVALID_TOKEN": (
        "Token o App ID inválidos o caducados. Genera un token nuevo en "
        "https://cedula.com.ve/web/login.php y actualiza API_TOKEN y API_APP_ID "
        "en Render (o en tu .env)."
    ),
    "INVALID_APP": "App ID no reconocido. Revisa API_APP_ID en las variables de entorno.",
}


def consultar_cedula(cedula: str, nacionalidad: str = "V") -> dict:
    params = {
        "app_id":       API_APP_ID,
        "token":        API_TOKEN,
        "nacionalidad": nacionalidad,
        "cedula":       cedula,
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("error"):
            code = (data.get("error_str") or "").strip()
            if code in _CEDULA_API_ERR_MSG:
                data = {**data, "error_str": _CEDULA_API_ERR_MSG[code]}
        return data
    except requests.exceptions.Timeout:
        return {"error": True, "error_str": "⏱️ Tiempo de espera agotado. Intenta de nuevo."}
    except requests.exceptions.ConnectionError:
        return {"error": True, "error_str": "🔌 Sin conexión al servidor. Intenta más tarde."}
    except Exception as e:
        return {"error": True, "error_str": f"Error inesperado: {str(e)}"}


def consultar_ivss(cedula: str, nacionalidad: str = "V") -> dict:
    """Consulta la Constancia de Cotizaciones del IVSS."""
    payload = {
        "nacionalidad": nacionalidad,
        "cedula":       cedula,
        "consultar":    "Buscar",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "http://www.ivss.gob.ve:28088/ConstanciaCotizacion/",
    }
    try:
        resp = requests.post(IVSS_URL, data=payload, headers=headers, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Verificar si hay mensaje de error / no encontrado
        error_tag = soup.find(string=lambda t: t and (
            "no se encontr" in t.lower() or
            "no existe" in t.lower() or
            "error" in t.lower()
        ))
        if error_tag:
            return {"error": True, "error_str": "❌ Cédula no encontrada en el IVSS."}

        # Extraer parámetros con expresiones regulares de la etiqueta de texto
        text = soup.get_text(separator=" ", strip=True)
        data = {}
        
        import re
        # Nombre (A veces aparece 'Ciudadano (a)')
        match = re.search(r'Ciudadano\s*\(a\)\s+([A-Z\sÑ]+?)\s+titular', text, re.IGNORECASE)
        if match: data["Nombre"] = match.group(1).strip()
        
        # Cedula
        match = re.search(r'Identidad N\s*[^\d]*(\d+)', text, re.IGNORECASE)
        if match: data["Cédula"] = match.group(1)
        
        # Semanas
        match = re.search(r'posee\s+(\d+)\s+semanas', text, re.IGNORECASE)
        if match: data["Semanas Cotizadas"] = match.group(1)
        
        # Afiliacion
        match = re.search(r'afiliaci[^ ]+\s+al\s+Instituto\s+([\d/]+)', text, re.IGNORECASE)
        if match: data["Fecha de Afiliación"] = match.group(1)
        
        # Estatus
        match = re.search(r'asegurado\s+([A-Z]+)\s+en\s+la\s+empresa', text, re.IGNORECASE)
        if match: data["Estatus"] = match.group(1)
        
        # Empresa
        match = re.search(r'en\s+la\s+empresa\s+(.+?)\s+inscrita', text, re.IGNORECASE)
        if match: data["Empresa"] = match.group(1).strip()
        
        # Patronal
        match = re.search(r'Patronal\s+([A-Z0-9]+)', text, re.IGNORECASE)
        if match: data["Número Patronal"] = match.group(1)
        
        # Egreso
        match = re.search(r'fecha de egreso\s+([\d/]+)', text, re.IGNORECASE)
        if match: data["Fecha de Egreso"] = match.group(1)

        if not data:
            return {"error": True, "error_str": "⚠️ El IVSS procesó la solicitud pero no se encontraron datos legibles."}

        return {"error": False, "data": data}

    except requests.exceptions.Timeout:
        return {"error": True, "error_str": "⏱️ El IVSS tardó demasiado. Intenta de nuevo."}
    except requests.exceptions.ConnectionError:
        return {"error": True, "error_str": "🔌 No se pudo conectar al servidor del IVSS."}
    except Exception as e:
        return {"error": True, "error_str": f"Error inesperado en IVSS: {str(e)}"}


def consultar_intt(cedula: str, nacionalidad: str = "V") -> dict:
    """Consulta vehículos en el portal del INTT usando peticiones Livewire."""
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "referer": INTT_LOGIN_URL
    }

    try:
        logger.info(f"INTT: Iniciando conexión para {nacionalidad}-{cedula}")
        # 1. Obtener tokens
        r1 = session.get(INTT_LOGIN_URL, headers=headers, timeout=20)
        soup1 = BeautifulSoup(r1.text, "html.parser")
        
        meta_token = soup1.find("meta", {"name": "csrf-token"})
        csrf_token = meta_token["content"] if meta_token else None
        
        div_login = soup1.find("div", attrs={"wire:snapshot": True})
        if not (csrf_token and div_login):
            logger.error("INTT: No se encontró CSRF o snapshot de login.")
            return {"error": True, "error_str": "Servidor INTT ocupado o no disponible."}
            
        snapshot_login = div_login["wire:snapshot"]
        
        # 2. Login
        headers.update({
            "x-csrf-token": csrf_token,
            "x-livewire": "true",
            "content-type": "application/json",
        })
        
        login_payload = {
            "_token": csrf_token,
            "components": [{
                "snapshot": snapshot_login,
                "updates": {
                    "email": INTT_USER,
                    "password": INTT_PASS
                },
                "calls": [{"path": "", "method": "save", "params": []}]
            }]
        }
        
        logger.info("INTT: Realizando login...")
        r2 = session.post(INTT_UPDATE_URL, json=login_payload, headers=headers, timeout=20)
        if r2.status_code != 200:
            logger.error(f"INTT: Error login status {r2.status_code}")
            return {"error": True, "error_str": "Fallo técnico al conectar con el portal."}

        # Verificar si hubo redirección (indicio de login exitoso)
        res_json_login = r2.json()
        effects = res_json_login.get("components", [{}])[0].get("effects", {})
        if "redirect" not in effects:
             logger.warning("INTT: Login no devolvió redirección. Posibles credenciales inválidas.")

        # 3. Página de vehículos
        logger.info("INTT: Accediendo a panel vehicular...")
        r3 = session.get(INTT_VEH_URL, headers=headers, timeout=20)
        if "Consulta de vehículos" not in r3.text:
            logger.error("INTT: No se pudo verificar acceso al panel vehicular.")
            return {"error": True, "error_str": "Acceso denegado al portal vehicular."}

        soup3 = BeautifulSoup(r3.text, "html.parser")
        div_veh = soup3.find("div", attrs={"wire:snapshot": True}) # Buscamos el componente principal
        if not div_veh:
            return {"error": True, "error_str": "No se encontró el buscador de vehículos."}
            
        snap_veh = div_veh["wire:snapshot"]

        # 4. Switch Tab
        logger.info("INTT: Seleccionando pestaña Cédula...")
        tab_payload = {
            "_token": csrf_token,
            "components": [{
                "snapshot": snap_veh,
                "updates": {},
                "calls": [{"path": "", "method": "switchTab", "params": ["cedula"]}]
            }]
        }
        r4 = session.post(INTT_UPDATE_URL, json=tab_payload, headers=headers, timeout=20)
        res_json4 = r4.json()
        snap_veh = res_json4.get("components", [{}])[0].get("snapshot")
        if not snap_veh:
             return {"error": True, "error_str": "Fallo al cambiar pestaña de búsqueda."}

        # 5. Buscar
        logger.info(f"INTT: Buscando {cedula}...")
        search_payload = {
            "_token": csrf_token,
            "components": [{
                "snapshot": snap_veh,
                "updates": {
                    "cedula": cedula,
                    "nacionalidad": nacionalidad
                },
                "calls": [{"path": "", "method": "buscar", "params": []}]
            }]
        }
        r5 = session.post(INTT_UPDATE_URL, json=search_payload, headers=headers, timeout=25)
        res_json5 = r5.json()
        comp_res = res_json5.get("components", [{}])[0]
        html_res = comp_res.get("effects", {}).get("html")
        
        if not html_res:
            logger.warning("INTT: La búsqueda no devolvió HTML de resultados (posible timeout).")
            return {"error": True, "error_str": "El portal no devolvió resultados a tiempo."}
        
        # 6. Parsear resultados
        res_soup = BeautifulSoup(html_res, "html.parser")
        
        # ¿Hay resultados? Si contiene "Vehículos registrados a nombre de"
        if "Vehículos registrados a nombre de" not in html_res and "No se encontraron" in html_res:
             return {"error": False, "owner": {}, "vehicles": []}

        owner = {}
        # Nombre: Buscamos el h6 que dice Nombre Completo
        for h6 in res_soup.find_all("h6"):
            txt = h6.get_text()
            if "Nombre Completo" in txt:
                owner["nombre"] = h6.find_next("p").get_text(strip=True)
            elif "Teléfono" in txt:
                owner["telefono"] = h6.find_next("p").get_text(strip=True)
            elif "Tipo de Sangre" in txt:
                owner["sangre"] = h6.find_next("p").get_text(strip=True)
            elif "Dirección" in txt:
                owner["direccion"] = h6.find_next("p").get_text(strip=True)

        vehicles = []
        rows = res_soup.find_all("tr")[1:]
        for row in rows:
            tds = row.find_all("td")
            if len(tds) >= 8:
                vehicles.append({
                    "placa":  tds[0].get_text(strip=True),
                    "serial": tds[1].get_text(strip=True),
                    "tipo":   tds[2].get_text(strip=True),
                    "marca":  tds[3].get_text(strip=True),
                    "modelo": tds[4].get_text(strip=True),
                    "color":  tds[5].get_text(strip=True),
                    "año":    tds[6].get_text(strip=True),
                    "estado": tds[7].get_text(strip=True),
                })

        logger.info(f"INTT: Consulta exitosa para {cedula} (Vehículos: {len(vehicles)})")
        return {"error": False, "owner": owner, "vehicles": vehicles}

    except Exception as e:
        logger.error(f"INTT CRITICAL ERROR: {str(e)}", exc_info=True)
        return {"error": True, "error_str": f"Error técnico en INTT: {str(e)}"}


def escape_md(text, is_code=False) -> str:
    """Escapa caracteres para Telegram MarkdownV2 según si están en un code block o no."""
    if text is None: return ""
    v = str(text)
    if is_code:
        # Dentro de bloques de código (backticks), solo se escapan backticks y contra barras.
        return v.replace("\\", "\\\\").replace("`", "\\`")
    else:
        # Fuera de bloques de código, se escapan todos los caracteres reservados.
        # IMPORTANTE: Escapar la contra barra '\' primero.
        for ch in r"\\_*[]()~`>#+-=|{}.!":
            v = v.replace(ch, f"\\{ch}")
        return v


def formatear_respuesta_intt(data: dict, nac: str, ced: str) -> str:
    """Formatea los resultados del INTT."""
    lin = []
    # Encabezado (escapado)
    lin.append(escape_md("╔══════════════════════════╗"))
    lin.append(escape_md("║  🚗  DATOS INTT (Vehículos) ║"))
    lin.append(escape_md("╚══════════════════════════╝"))
    lin.append("")
    lin.append(f"🪪  *{escape_md('Cédula:')}*  `{escape_md(nac, True)}-{escape_md(ced, True)}`")
    lin.append("")

    owner = data.get("owner", {})
    if owner:
        lin.append(f"👤 *{escape_md('PROPIETARIO:')}*")
        lin.append(f"   • {escape_md('Nombre:')} `{escape_md(owner.get('nombre', '—'), True)}`")
        if owner.get("telefono") and owner["telefono"] != "No disponible":
            lin.append(f"   • {escape_md('Teléfono:')} `{escape_md(owner['telefono'], True)}`")
        if owner.get("sangre") and owner["sangre"] != "No disponible":
            lin.append(f"   • {escape_md('Sangre:')} `{escape_md(owner['sangre'], True)}`")
        if owner.get("direccion") and owner["direccion"] != "No disponible":
            lin.append(f"   • {escape_md('Dirección:')} `{escape_md(owner['direccion'], True)}`")
        lin.append("")

    vehicles = data.get("vehicles", [])
    if not vehicles:
        lin.append(f"❌ *{escape_md('No se encontraron vehículos registrados.')}*")
    else:
        for i, veh in enumerate(vehicles, 1):
            lin.append(f"🚘 *{escape_md(f'Vehículo #{i}:')}*")
            lin.append(f"   📟 {escape_md('Placa:')} `{escape_md(veh.get('placa'), True)}`")
            lin.append(f"   🔢 {escape_md('Serial:')} `{escape_md(veh.get('serial'), True)}`")
            lin.append(f"   🚛 {escape_md('Tipo:')} `{escape_md(veh.get('tipo'), True)}`")
            lin.append(f"   🏢 {escape_md('Marca:')} `{escape_md(veh.get('marca'), True)}`")
            lin.append(f"   🚗 {escape_md('Modelo:')} `{escape_md(veh.get('modelo'), True)}`")
            lin.append(f"   🎨 {escape_md('Color:')} `{escape_md(veh.get('color'), True)}`")
            lin.append(f"   📅 {escape_md('Año:')} `{escape_md(veh.get('año'), True)}`")
            lin.append(f"   🔖 {escape_md('Estado:')} `{escape_md(veh.get('estado'), True)}`")
            lin.append("")

    return "\n".join(lin)


def formatear_respuesta(data: dict) -> str:
    nac   = data.get("nacionalidad", "V")
    ced   = data.get("cedula", "—")
    rif   = data.get("rif", "—")

    p_ap  = data.get("primer_apellido",  "")
    s_ap  = data.get("segundo_apellido", "")
    p_nom = data.get("primer_nombre",    "")
    s_nom = data.get("segundo_nombre",   "")
    nombre = f"{p_nom} {s_nom} {p_ap} {s_ap}".strip()

    cne       = data.get("cne", {})
    estado    = cne.get("estado",           "—")
    municipio = cne.get("municipio",        "—")
    parroquia = cne.get("parroquia",        "—")
    centro    = cne.get("centro_electoral", "—")

    return (
        escape_md("╔══════════════════════════╗") + "\n" +
        escape_md("║  📋  DATOS ENCONTRADOS   ║") + "\n" +
        escape_md("╚══════════════════════════╝") + "\n\n" +
        f"🪪  *{escape_md('Cédula:')}*          `{escape_md(nac, True)}-{escape_md(ced, True)}`" + "\n" +
        f"🧾  *{escape_md('R.I.F.:')}*         `{escape_md(rif, True)}`" + "\n" +
        f"👤  *{escape_md('Nombre:')}*          `{escape_md(nombre, True)}`" + "\n\n" +
        f"🗳️  *{escape_md('Datos CNE')}*\n" +
        f"    📍 {escape_md('Estado:')}          `{escape_md(estado, True)}`" + "\n" +
        f"    🏘️  {escape_md('Municipio:')}      `{escape_md(municipio, True)}`" + "\n" +
        f"    ⛪ {escape_md('Parroquia:')}       `{escape_md(parroquia, True)}`" + "\n" +
        f"    🏫 {escape_md('Centro Electoral:')}\n" +
        f"       `{escape_md(centro, True)}`" + "\n"
    )


def formatear_respuesta_ivss(data: dict, nac: str, ced: str) -> str:
    """Convierte el diccionario del IVSS en texto MarkdownV2."""
    lin = []
    lin.append(escape_md("╔══════════════════════════╗"))
    lin.append(escape_md("║  🏥  DATOS IVSS          ║"))
    lin.append(escape_md("╚══════════════════════════╝"))
    lin.append("")
    lin.append(f"🪪  *{escape_md('Cédula:')}*  `{escape_md(nac, True)}-{escape_md(ced, True)}`")
    lin.append("")

    claves_emoji = {
        "semanas cotizadas":   "📊",
        "afiliacion":          "📅", "afiliación": "📅",
        "estatus":             "🔖", "status": "🔖",
        "empresa":             "🏢", "empleador": "🏢",
        "patronal":            "🔢",
        "egreso":              "📤",
        "vigencia":            "⏳",
    }

    for key, val in data.items():
        if not val: continue
        key_lower = key.lower()
        emoji = next((v for k, v in claves_emoji.items() if k in key_lower), "▪️")
        lin.append(f"{emoji}  *{escape_md(key)}:*")
        lin.append(f"    `{escape_md(val, True)}`")

    return "\n".join(lin)


def _parse_cedula_arg(raw: str) -> tuple[str | None, str | None]:
    value = (raw or "").strip().upper()
    if not value:
        return None, None
    if value[0] in ("V", "E", "J", "G", "P") and value[1:].isdigit():
        return value[0], value[1:]
    if value.isdigit():
        return "V", value
    return None, None


def _seniat_horario_abierto() -> bool:
    # SENIAT publica ventana diaria de consulta entre 09:00 y 20:59 (hora VE).
    ve_now = datetime.utcnow()
    hora_ve = (ve_now.hour - 4) % 24
    minutos_ve = hora_ve * 60 + ve_now.minute
    return 9 * 60 <= minutos_ve <= (20 * 60 + 59)


def consultar_seniat(cedula: str, nacionalidad: str = "V") -> dict:
    if not _seniat_horario_abierto():
        return {
            "error": True,
            "error_str": "Consulta SENIAT disponible de 09:00 a 20:59 (hora Venezuela).",
        }

    personalidad_map = {"V": "1", "E": "2", "J": "3", "P": "4", "G": "5"}
    personalidad = personalidad_map.get((nacionalidad or "V").upper(), "1")
    max_reintentos = 5
    ultimo_error: str | None = None

    for intento in range(1, max_reintentos + 1):
        session = requests.Session()
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            inicio = session.get(SENIAT_URL, headers=headers, timeout=30)
            inicio.raise_for_status()
            inicio.encoding = "windows-1252"
            soup_inicio = BeautifulSoup(inicio.text, "html.parser")

            # SENIAT suele devolver action con jsessionid; usarla mejora estabilidad.
            form = soup_inicio.find("form", attrs={"name": "rifRelacionConsultaForm"})
            action = form.get("action") if form else "/relacionesrif/inicioConsulta.do"
            if not action.startswith("http"):
                action = f"http://contribuyente.seniat.gob.ve{action}"
            action_fallback = "http://contribuyente.seniat.gob.ve/relacionesrif/inicioConsulta.do"

            payload = {
                "contexto": "/relacionesrif",
                "personalidad": personalidad,
                "ci": cedula,
            }
            post_headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": SENIAT_URL,
                "Connection": "close",
            }
            try:
                resp = session.post(
                    action,
                    data=payload,
                    headers=post_headers,
                    timeout=(20, 45),
                )
            except requests.exceptions.Timeout:
                # Fallback en el mismo intento por si el action con jsessionid quedó colgado.
                resp = session.post(
                    action_fallback,
                    data=payload,
                    headers=post_headers,
                    timeout=(20, 45),
                )
            resp.raise_for_status()
            resp.encoding = "windows-1252"

            soup = BeautifulSoup(resp.text, "html.parser")
            plain = " ".join(soup.stripped_strings)

            if "Rif Errado" in plain:
                return {
                    "error": True,
                    "error_str": (
                        "Identificación inválida para SENIAT (RIF errado). "
                        "Verifica prefijo (V/E/J/G) y número."
                    ),
                }

            if "No se encontraron Contribuyentes Relacionados" in plain:
                relacion = "No posee relación"
            else:
                relacion = "Posee relación"

            id_match = re.search(r"Cédula o Rif:\s*([VEJPG]-\d{5,9}-\d)", plain, re.IGNORECASE)
            nom_match = re.search(r"Nombre:\s*([A-ZÁÉÍÓÚÑ,\s]+?)\s+(No posee relación|Posee relación)", plain, re.IGNORECASE)

            if not id_match and not nom_match:
                if "mensajeError" in resp.text or "error" in plain.lower():
                    return {"error": True, "error_str": "SENIAT devolvió un error en la consulta."}
                return {"error": True, "error_str": "No se pudo interpretar la respuesta de SENIAT."}

            return {
                "error": False,
                "data": {
                    "rif": id_match.group(1).upper() if id_match else f"{nacionalidad}-{cedula}",
                    "nombre": " ".join((nom_match.group(1) if nom_match else "No disponible").split()),
                    "relacion": relacion,
                },
            }
        except requests.exceptions.Timeout:
            ultimo_error = "timeout"
            logger.warning(
                "SENIAT timeout intento %s/%s para %s-%s",
                intento,
                max_reintentos,
                nacionalidad,
                cedula,
            )
            if intento < max_reintentos:
                time.sleep(min(6, 2 ** (intento - 1)))
                continue
        except requests.exceptions.ConnectionError:
            ultimo_error = "conexion"
            logger.warning(
                "SENIAT conexión fallida intento %s/%s para %s-%s",
                intento,
                max_reintentos,
                nacionalidad,
                cedula,
            )
            if intento < max_reintentos:
                time.sleep(min(6, 2 ** (intento - 1)))
                continue
        except Exception as e:
            return {"error": True, "error_str": f"Error inesperado en SENIAT: {str(e)}"}

    if ultimo_error == "timeout":
        return {"error": True, "error_str": "SENIAT tardó demasiado en responder (se reintentó 5 veces)."}
    if ultimo_error == "conexion":
        return {"error": True, "error_str": "No se pudo conectar al servidor de SENIAT (se reintentó 5 veces)."}
    return {"error": True, "error_str": "No fue posible completar la consulta SENIAT."}


def formatear_respuesta_seniat(data: dict, nac: str, ced: str) -> str:
    lin = []
    lin.append("╔══════════════════════════╗")
    lin.append("║  🧾  DATOS SENIAT        ║")
    lin.append("╚══════════════════════════╝")
    lin.append("")
    lin.append(f"🪪  Cédula:  {nac}-{ced}")
    lin.append(f"🧾  RIF:     {data.get('rif', '—')}")
    lin.append(f"👤  Nombre:  {data.get('nombre', '—')}")
    lin.append(f"📌  Relación: {data.get('relacion', '—')}")
    return "\n".join(lin)


# ─────────────────────────────────────────────
#  Handlers
# ─────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    nombre = update.effective_user.first_name
    texto = (
        f"👋 ¡Hola, *{nombre}*\\!\n\n"
        "Soy el bot de consulta de *Cédulas Venezolanas* 🇻🇪\n\n"
        "📌 *¿Qué puedo hacer?*\n"
        "  • Consultar datos por número de cédula\n"
        "  • Mostrar nombre, RIF y estado donde vota\n"
        "  • Datos del IVSS \\(semanas cotizadas\\)\n"
        "  • Vehículos registrados \\(INTT\\)\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📲 *Comandos disponibles:*\n"
        "  /consultar — Iniciar una consulta\n"
        "  /digitel    — Buscar en base Digitel \\(tel / documento\\)\n"
        "  /gnb        — Buscar en base GNB \\(cédula o nombre\\)\n"
        "  /cicpc      — Buscar en base CICPC \\(cédula o nombre\\)\n"
        "  /pnb        — Buscar en base PNB \\(cédula o nombre\\)\n"
        "  /seniat     — Consultar datos en SENIAT \\(09:00–20:59 VE\\)\n"
        "  /exportar\\_chat — Descargar PDF del historial registrado\n"
        "  /olvidar\\_historial — Borrar historial guardado en el servidor\n"
        "  /help       — Ayuda\n"
        "  /start      — Volver al inicio\n\n"
        "O simplemente envíame un número de cédula directamente 👇"
    )
    await update.message.reply_text(texto, parse_mode="MarkdownV2")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    texto = (
        "🆘 *Ayuda \\- Bot Cédula Venezuela*\n\n"
        "Puedes consultar una cédula de dos formas:\n\n"
        "1️⃣ Escribe directamente el número:\n"
        "   Ejemplo: `23775072`\n\n"
        "2️⃣ Usa el comando:\n"
        "   `/consultar 23775072`\n\n"
        "Para cédulas extranjeras agrega la letra E:\n"
        "   `/consultar E1234567`\n\n"
        "📡 Digitel: `/digitel` · GNB: `/gnb` · CICPC: `/cicpc` · PNB: `/pnb` · SENIAT: `/seniat`\n"
        "📄 Historial: `/exportar\\_chat` · 🗑 `/olvidar\\_historial`\n\n"
        "ℹ️ Datos que obtienes:\n"
        "  • Nombre completo\n"
        "  • R\\.I\\.F\\.\n"
        "  • Estado donde vota\n"
        "  • Municipio, Parroquia y Centro Electoral\n"
    )
    await update.message.reply_text(texto, parse_mode="MarkdownV2")


def _digitel_parse_args(args: list[str]) -> tuple[str | None, str | None, bool]:
    """
    Devuelve (modo, valor, necesita_texto_ayuda).
    modo: t / d (interno). Un solo argumento solo dígitos: >=11 o empieza por 58 → teléfono; 5–10 → documento.
    """
    if not args:
        return None, None, True
    if len(args) >= 2:
        return args[0].lower(), " ".join(args[1:]), False
    solo = "".join(c for c in args[0] if c.isdigit())
    if not solo:
        return None, None, True
    if len(solo) >= 11 or solo.startswith("58"):
        return "t", solo, False
    if 5 <= len(solo) <= 10:
        return "d", solo, False
    return None, None, True


async def digitel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Consulta la base Digitel importada en SQLite (índices en teléfono y documento)."""
    from digitel_sqlite import (
        buscar_por_documento,
        buscar_por_telefono,
        db_path,
        ensure_digitel_database,
    )

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return

    args = list(context.args or [])
    mode, valor, need_help = _digitel_parse_args(args)
    if need_help or not mode or not valor:
        await msg.reply_text(
            "Uso:\n"
            "  /digitel t <telefono>   — ejemplo: /digitel t 584123280905\n"
            "  /digitel d <documento> — solo dígitos, ej: /digitel d 303591710\n"
            "  /digitel 584123280905  — solo número largo (58…) = teléfono\n"
            "  /digitel 303591710     — 5–10 dígitos = documento\n\n"
            "Abreviaturas: t / tel / telefono · d / doc / documento"
        )
        return

    if mode not in ("t", "tel", "telefono", "d", "doc", "documento"):
        await msg.reply_text(
            "Indica modo `t` (teléfono) o `d` (documento) primero."
        )
        return

    try:
        if not db_path().is_file():
            await msg.reply_text(
                "⏳ Descargando la base Digitel la primera vez "
                "(varios minutos si el archivo es grande). No cierres el chat…"
            )
        await asyncio.to_thread(ensure_digitel_database)
    except FileNotFoundError as e:
        await msg.reply_text(f"❌ {e}")
        return
    except Exception as e:
        logger.exception("Digitel: fallo al preparar la base: %s", e)
        await msg.reply_text(
            "❌ Error al preparar la base Digitel. Revisa DIGITEL_DOWNLOAD_URL / "
            "DIGITEL_DB y los logs del servidor."
        )
        return

    if not db_path().is_file():
        await msg.reply_text(
            "📂 No hay base Digitel en esta máquina.\n\n"
            "• Local: `python import_digitel_sqlite.py`\n"
            "• Nube: variables `DIGITEL_DOWNLOAD_URL` (URL pública del .sqlite) y "
            "`DIGITEL_DB` (ruta escribible, ej. `/tmp/digitel.sqlite`)."
        )
        return

    try:
        if mode in ("t", "tel", "telefono"):
            rows, trunc = buscar_por_telefono(valor)
        else:
            rows, trunc = buscar_por_documento(valor)
    except FileNotFoundError as e:
        await msg.reply_text(str(e))
        return
    except Exception as e:
        logger.exception("Digitel: error en consulta: %s", e)
        await msg.reply_text("❌ Error al consultar Digitel.")
        return

    if not rows:
        await msg.reply_text("Sin resultados en Digitel.")
        return

    max_lines = 40
    lines = ["📋 Digitel"]
    for r in rows[:max_lines]:
        lines.append(f"{r['tipo']}  {r['documento']}  {r['telefono']}")
    if len(rows) > max_lines:
        lines.append(f"\n… (+{len(rows) - max_lines} filas más en este resultado)")
    if trunc:
        lines.append("… (hay más coincidencias en la base; límite 100 por consulta)")

    await msg.reply_text("\n".join(lines))


def _gnb_parse_args(args: list[str]) -> tuple[str | None, str | None, bool]:
    """
    (modo, valor, necesita_ayuda). modo: 'c' cédula, 'n' fragmento nombre.
    """
    if not args:
        return None, None, True
    if len(args) == 1:
        solo = "".join(c for c in args[0] if c.isdigit())
        if solo and 4 <= len(solo) <= 12:
            return "c", solo, False
        frag = args[0].strip()
        if len(frag) >= 3:
            return "n", frag, False
        return None, None, True
    head = args[0].lower()
    rest = " ".join(args[1:]).strip()
    if not rest:
        return None, None, True
    if head in ("c", "cedula", "ced"):
        dig = "".join(c for c in rest if c.isdigit())
        return ("c", dig or rest, False) if (dig or rest).strip() else (None, None, True)
    if head in ("n", "nombre", "a", "apellido", "apellidos"):
        return "n", rest, False
    return None, None, True


async def gnb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Consulta la base GNB (SQLite): por cédula o por fragmento de nombre."""
    from gnb_sqlite import (
        buscar_por_cedula,
        buscar_por_nombre,
        compactar_fila,
        db_path,
        ensure_gnb_database,
    )

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return

    args = list(context.args or [])
    mode, valor, need_help = _gnb_parse_args(args)
    if need_help or not mode or not valor:
        await msg.reply_text(
            "Uso GNB:\n"
            "  /gnb <cédula>     — ejemplo: /gnb 6857541\n"
            "  /gnb c <cédula>   — igual, solo dígitos\n"
            "  /gnb n <texto>    — buscar en apellidos/nombre (mín. 3 letras)\n"
            "  /gnb nombre PEREZ — ejemplo por nombre\n\n"
            "En Render: `GNB_DB` y opcional `GNB_DOWNLOAD_URL` (Release con gnb.sqlite)."
        )
        return

    try:
        if not db_path().is_file():
            await msg.reply_text(
                "⏳ Descargando la base GNB la primera vez "
                "(puede tardar si el archivo es grande)…"
            )
        await asyncio.to_thread(ensure_gnb_database)
    except FileNotFoundError as e:
        await msg.reply_text(f"❌ {e}")
        return
    except Exception as e:
        logger.exception("GNB: fallo al preparar la base: %s", e)
        await msg.reply_text(
            "❌ Error al preparar GNB. Revisa GNB_DOWNLOAD_URL / GNB_DB en el servidor."
        )
        return

    if not db_path().is_file():
        await msg.reply_text(
            "📂 No hay base GNB.\n\n"
            "• Local: `python import_gnb_sqlite.py`\n"
            "• Nube: `GNB_DOWNLOAD_URL` + `GNB_DB` (ej. `/tmp/gnb.sqlite`)."
        )
        return

    try:
        if mode == "c":
            rows = buscar_por_cedula(valor)
            trunc = False
        else:
            rows, trunc = buscar_por_nombre(valor)
    except FileNotFoundError as e:
        await msg.reply_text(str(e))
        return
    except Exception as e:
        logger.exception("GNB: error en consulta: %s", e)
        await msg.reply_text("❌ Error al consultar GNB.")
        return

    if not rows:
        await msg.reply_text("Sin resultados en GNB.")
        return

    bloques: list[str] = ["📋 GNB"]
    max_filas = 6 if mode == "n" else 3
    for i, r in enumerate(rows[:max_filas], 1):
        bloques.append(f"\n── #{i} ──\n{compactar_fila(r)}")
    if len(rows) > max_filas:
        bloques.append(f"\n… (+{len(rows) - max_filas} filas más)")
    if trunc:
        bloques.append("\n… (hay más coincidencias; muestra limitada)")

    texto = "\n".join(bloques)
    if len(texto) > 4000:
        texto = texto[:3990] + "\n… (mensaje recortado)"
    await msg.reply_text(texto)


def _cicpc_parse_args(args: list[str]) -> tuple[str | None, str | None, bool]:
    """
    (modo, valor, necesita_ayuda). modo: 'c' cédula, 'n' fragmento nombre.
    """
    if not args:
        return None, None, True
    if len(args) == 1:
        token = args[0].strip().upper()
        if len(token) >= 2 and token[0] in ("V", "E") and token[1:].isdigit():
            return "c", token, False
        solo = "".join(c for c in token if c.isdigit())
        if solo and 4 <= len(solo) <= 12:
            return "c", solo, False
        frag = args[0].strip()
        if len(frag) >= 3:
            return "n", frag, False
        return None, None, True
    head = args[0].lower()
    rest = " ".join(args[1:]).strip()
    if not rest:
        return None, None, True
    if head in ("c", "cedula", "ced"):
        rest_up = rest.upper()
        if len(rest_up) >= 2 and rest_up[0] in ("V", "E") and rest_up[1:].isdigit():
            return "c", rest_up, False
        dig = "".join(c for c in rest if c.isdigit())
        return ("c", dig or rest, False) if (dig or rest).strip() else (None, None, True)
    if head in ("n", "nombre", "a", "apellido", "apellidos"):
        return "n", rest, False
    return None, None, True


async def cicpc_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Consulta la base CICPC (SQLite): por cédula o por fragmento de nombre."""
    from cicpc_sqlite import (
        buscar_por_cedula,
        buscar_por_documento,
        buscar_por_nombre,
        compactar_fila,
        db_path,
        ensure_cicpc_database,
    )

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return

    args = list(context.args or [])
    mode, valor, need_help = _cicpc_parse_args(args)
    if need_help or not mode or not valor:
        await msg.reply_text(
            "Uso CICPC:\n"
            "  /cicpc <cédula>      — ejemplo: /cicpc 17965814\n"
            "  /cicpc V17965814     — cédula con nacionalidad\n"
            "  /cicpc c <cédula>    — igual, solo dígitos\n"
            "  /cicpc c E12345678   — modo cédula con prefijo\n"
            "  /cicpc n <texto>     — buscar en nombre/apellido (mín. 3 letras)\n"
            "  /cicpc nombre PUERTA — ejemplo por nombre\n\n"
            "En Render: `CICPC_DB` y opcional `CICPC_DOWNLOAD_URL`."
        )
        return

    try:
        if not db_path().is_file():
            await msg.reply_text(
                "⏳ Descargando la base CICPC la primera vez "
                "(puede tardar si el archivo es grande)…"
            )
        await asyncio.to_thread(ensure_cicpc_database)
    except FileNotFoundError as e:
        await msg.reply_text(f"❌ {e}")
        return
    except Exception as e:
        logger.exception("CICPC: fallo al preparar la base: %s", e)
        await msg.reply_text(
            "❌ Error al preparar CICPC. Revisa CICPC_DOWNLOAD_URL / CICPC_DB en el servidor."
        )
        return

    if not db_path().is_file():
        await msg.reply_text(
            "📂 No hay base CICPC.\n\n"
            "• Local: `python import_cicpc_sqlite.py`\n"
            "• Nube: `CICPC_DOWNLOAD_URL` + `CICPC_DB` (ej. `/tmp/cicpc.sqlite`)."
        )
        return

    try:
        if mode == "c":
            val_up = valor.strip().upper()
            if len(val_up) >= 2 and val_up[0] in ("V", "E") and val_up[1:].isdigit():
                rows = buscar_por_documento(val_up)
            else:
                rows = buscar_por_cedula(valor)
            trunc = False
        else:
            rows, trunc = buscar_por_nombre(valor)
    except FileNotFoundError as e:
        await msg.reply_text(str(e))
        return
    except Exception as e:
        logger.exception("CICPC: error en consulta: %s", e)
        await msg.reply_text("❌ Error al consultar CICPC.")
        return

    if not rows:
        await msg.reply_text("Sin resultados en CICPC.")
        return

    bloques: list[str] = ["📋 CICPC"]
    max_filas = 6 if mode == "n" else 3
    for i, r in enumerate(rows[:max_filas], 1):
        bloques.append(f"\n── #{i} ──\n{compactar_fila(r)}")
    if len(rows) > max_filas:
        bloques.append(f"\n… (+{len(rows) - max_filas} filas más)")
    if trunc:
        bloques.append("\n… (hay más coincidencias; muestra limitada)")

    texto = "\n".join(bloques)
    if len(texto) > 4000:
        texto = texto[:3990] + "\n… (mensaje recortado)"
    await msg.reply_text(texto)


def _pnb_parse_args(args: list[str]) -> tuple[str | None, str | None, bool]:
    if not args:
        return None, None, True
    if len(args) == 1:
        token = args[0].strip().upper()
        if len(token) >= 2 and token[0] in ("V", "E") and token[1:].isdigit():
            return "c", token, False
        solo = "".join(c for c in token if c.isdigit())
        if solo and 4 <= len(solo) <= 12:
            return "c", solo, False
        frag = args[0].strip()
        if len(frag) >= 3:
            return "n", frag, False
        return None, None, True
    head = args[0].lower()
    rest = " ".join(args[1:]).strip()
    if not rest:
        return None, None, True
    if head in ("c", "cedula", "ced"):
        rest_up = rest.upper()
        if len(rest_up) >= 2 and rest_up[0] in ("V", "E") and rest_up[1:].isdigit():
            return "c", rest_up, False
        dig = "".join(c for c in rest if c.isdigit())
        return ("c", dig or rest, False) if (dig or rest).strip() else (None, None, True)
    if head in ("n", "nombre", "a", "apellido", "apellidos"):
        return "n", rest, False
    return None, None, True


async def pnb_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    from pnb_sqlite import (
        buscar_por_cedula,
        buscar_por_documento,
        buscar_por_nombre,
        compactar_fila,
        db_path,
        ensure_pnb_database,
    )

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return

    args = list(context.args or [])
    mode, valor, need_help = _pnb_parse_args(args)
    if need_help or not mode or not valor:
        await msg.reply_text(
            "Uso PNB:\n"
            "  /pnb <cédula>      — ejemplo: /pnb 21164708\n"
            "  /pnb V21164708     — cédula con nacionalidad\n"
            "  /pnb c <cédula>    — igual, solo dígitos\n"
            "  /pnb c E12345678   — modo cédula con prefijo\n"
            "  /pnb n <texto>     — buscar en nombre/apellido (mín. 3 letras)\n"
            "  /pnb nombre TORRES — ejemplo por nombre\n\n"
            "En Render: `PNB_DB` y opcional `PNB_DOWNLOAD_URL`."
        )
        return

    try:
        if not db_path().is_file():
            await msg.reply_text(
                "⏳ Descargando la base PNB la primera vez "
                "(puede tardar si el archivo es grande)…"
            )
        await asyncio.to_thread(ensure_pnb_database)
    except FileNotFoundError as e:
        await msg.reply_text(f"❌ {e}")
        return
    except Exception as e:
        logger.exception("PNB: fallo al preparar la base: %s", e)
        await msg.reply_text(
            "❌ Error al preparar PNB. Revisa PNB_DOWNLOAD_URL / PNB_DB en el servidor."
        )
        return

    if not db_path().is_file():
        await msg.reply_text(
            "📂 No hay base PNB.\n\n"
            "• Local: `python import_pnb_sqlite.py`\n"
            "• Nube: `PNB_DOWNLOAD_URL` + `PNB_DB` (ej. `/tmp/pnb.sqlite`)."
        )
        return

    try:
        if mode == "c":
            val_up = valor.strip().upper()
            if len(val_up) >= 2 and val_up[0] in ("V", "E") and val_up[1:].isdigit():
                rows = buscar_por_documento(val_up)
            else:
                rows = buscar_por_cedula(valor)
            trunc = False
        else:
            rows, trunc = buscar_por_nombre(valor)
    except FileNotFoundError as e:
        await msg.reply_text(str(e))
        return
    except Exception as e:
        logger.exception("PNB: error en consulta: %s", e)
        await msg.reply_text("❌ Error al consultar PNB.")
        return

    if not rows:
        await msg.reply_text("Sin resultados en PNB.")
        return

    bloques: list[str] = ["📋 PNB"]
    max_filas = 6 if mode == "n" else 3
    for i, r in enumerate(rows[:max_filas], 1):
        bloques.append(f"\n── #{i} ──\n{compactar_fila(r)}")
    if len(rows) > max_filas:
        bloques.append(f"\n… (+{len(rows) - max_filas} filas más)")
    if trunc:
        bloques.append("\n… (hay más coincidencias; muestra limitada)")

    texto = "\n".join(bloques)
    if len(texto) > 4000:
        texto = texto[:3990] + "\n… (mensaje recortado)"
    await msg.reply_text(texto)


async def seniat_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return

    args = list(context.args or [])
    if not args:
        await msg.reply_text(
            "Uso:\n"
            "  /seniat <identificacion>\n"
            "  /seniat V23775072\n"
            "  /seniat E1234567\n\n"
            "También acepta J y G (ej: /seniat J123456789).\n"
            "Horario SENIAT: 09:00 a 20:59 (hora Venezuela)."
        )
        return

    nac, ced = _parse_cedula_arg(args[0])
    if not nac or not ced or not (5 <= len(ced) <= 9):
        await msg.reply_text(
            "Formato inválido. Usa solo dígitos o prefijo V/E/J/G.\n"
            "Ejemplos: /seniat 23775072 · /seniat V23775072 · /seniat J123456789"
        )
        return

    wait = await msg.reply_text("🧾 Consultando SENIAT…")
    result = await asyncio.to_thread(consultar_seniat, ced, nac)
    if result.get("error"):
        await wait.edit_text(f"❌ SENIAT: {result.get('error_str', 'Error desconocido.')}")
        return

    texto = formatear_respuesta_seniat(result.get("data", {}), nac, ced)
    await wait.edit_text(texto)


async def consultar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.args:
        raw = context.args[0].strip().upper()
        await procesar_cedula_raw(update, context, raw)
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton("🇻🇪 Venezolana (V)", callback_data="NAC_V"),
         InlineKeyboardButton("🌐 Extranjera (E)",   callback_data="NAC_E")],
    ]
    await update.message.reply_text(
        "¿Qué tipo de cédula vas a consultar?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return ESPERANDO_CEDULA


async def nacionalidad_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_user_allowed(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    nacionalidad = query.data.replace("NAC_", "")
    context.user_data["nacionalidad"] = nacionalidad
    emoji = "🇻🇪" if nacionalidad == "V" else "🌐"
    await query.edit_message_text(
        f"{emoji} Cédula *{nacionalidad}* seleccionada\\.\n\n"
        "Ahora envíame el número de cédula \\(solo dígitos\\):",
        parse_mode="MarkdownV2",
    )
    return ESPERANDO_CEDULA


async def recibir_cedula(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await procesar_cedula_raw(update, context, update.message.text.strip().upper())
    return ConversationHandler.END


async def _enviar_seniat_diferido(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    nacionalidad: str,
    cedula: str,
) -> None:
    """Ejecuta SENIAT en segundo plano y envía solo resultados útiles."""
    loop = asyncio.get_event_loop()
    try:
        result_seniat = await asyncio.wait_for(
            loop.run_in_executor(None, consultar_seniat, cedula, nacionalidad),
            timeout=120,
        )
        if result_seniat.get("error"):
            error_seniat = result_seniat.get("error_str", "Error desconocido.")
            # En /consultar evitamos ruido cuando SENIAT está intermitente.
            # Solo notificamos errores de validación útiles para el usuario.
            if "RIF errado" in error_seniat or "Identificación inválida" in error_seniat:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ 🧾 SENIAT: {error_seniat}")
        else:
            txt_seniat = formatear_respuesta_seniat(result_seniat.get("data", {}), nacionalidad, cedula)
            await context.bot.send_message(chat_id=chat_id, text=txt_seniat)
    except asyncio.TimeoutError:
        logger.warning("SENIAT diferido sin respuesta a tiempo para %s-%s", nacionalidad, cedula)
    except Exception as e:
        logger.error("Error en SENIAT diferido: %s", e, exc_info=True)
        # No enviamos error técnico al usuario en /consultar para no degradar UX.


async def procesar_cedula_raw(update, context, raw: str) -> None:
    # Detectar prefijo V/E
    if raw.startswith("V") and raw[1:].isdigit():
        nacionalidad, cedula = "V", raw[1:]
    elif raw.startswith("E") and raw[1:].isdigit():
        nacionalidad, cedula = "E", raw[1:]
    elif raw.isdigit():
        nacionalidad = context.user_data.get("nacionalidad", "V")
        cedula = raw
    else:
        await update.message.reply_text(
            "⚠️ Formato inválido\\. Envía solo el número de cédula\\.\n"
            "Ejemplo: `23775072` o `V23775072`",
            parse_mode="MarkdownV2",
        )
        return

    if not (5 <= len(cedula) <= 9):
        await update.message.reply_text(
            "⚠️ La cédula debe tener entre 5 y 9 dígitos\\.",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🔍 Consultando *🗳️ CNE · 🏥 IVSS · 🚗 INTT · 🧾 SENIAT* en paralelo\\.\\.\\. un momento ⏳",
        parse_mode="MarkdownV2",
    )

    # Consultar CNE, IVSS e INTT en paralelo (respuesta principal rápida)
    loop = asyncio.get_event_loop()
    result_cedula, result_ivss, result_intt = await asyncio.gather(
        loop.run_in_executor(None, consultar_cedula, cedula, nacionalidad),
        loop.run_in_executor(None, consultar_ivss, cedula, nacionalidad),
        loop.run_in_executor(None, consultar_intt, cedula, nacionalidad),
    )

    # ── Bloque 1: Cédula / CNE ─────────────────────────────────────────
    if result_cedula.get("error"):
        error = result_cedula.get("error_str", "Error desconocido.")
        await msg.edit_text(f"❌ *🗳️ CNE:* `{error}`", parse_mode="MarkdownV2")
    else:
        data_cedula = result_cedula.get("data", {})
        if data_cedula:
            keyboard = [[InlineKeyboardButton("🔄 Nueva consulta", callback_data="NUEVA_CONSULTA")]]
            await msg.edit_text(
                formatear_respuesta(data_cedula),
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        else:
            await msg.edit_text("❌ Cédula no encontrada en CNE\\.", parse_mode="MarkdownV2")

    # ── Bloque 2: IVSS ──────────────────────────────────────────────────
    if result_ivss.get("error"):
        error_ivss = result_ivss.get("error_str", "Error en IVSS.")
        await update.message.reply_text(
            f"⚠️ *🏥 IVSS:* `{escape_md(error_ivss, True)}`",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            formatear_respuesta_ivss(result_ivss.get("data", {}), nacionalidad, cedula),
            parse_mode="MarkdownV2",
        )

    # ── Bloque 3: INTT (Vehículos) ─────────────────────────────────────
    try:
        if result_intt.get("error"):
            error_intt = result_intt.get("error_str", "Error desconocido.")
            await update.message.reply_text(
                f"⚠️ *🚗 INTT:* `{escape_md(error_intt, True)}`",
                parse_mode="MarkdownV2",
            )
        else:
            txt_intt = formatear_respuesta_intt(result_intt, nacionalidad, cedula)
            await update.message.reply_text(txt_intt, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error enviando mensaje INTT: {e}")
        await update.message.reply_text("❌ *🚗 INTT:* El mensaje contiene caracteres no compatibles o hubo un fallo al enviarlo\\.", parse_mode="MarkdownV2")

    # ── Bloque 4: SENIAT en segundo plano (no bloquea /consultar) ───────────
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is not None:
        await update.message.reply_text(
            "🧾 SENIAT sigue procesando en segundo plano\\. Si responde, te envío el resultado en este chat\\.",
            parse_mode="MarkdownV2",
        )
        asyncio.create_task(_enviar_seniat_diferido(context, chat_id, nacionalidad, cedula))
    else:
        await update.message.reply_text("⚠️ 🧾 SENIAT: no se pudo determinar el chat para respuesta diferida.")

    logger.info("Consulta completa: %s-%s", nacionalidad, cedula)


async def nueva_consulta_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_user_allowed(update):
        return
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "✏️ Envíame el número de cédula a consultar:\n\n"
        "Ejemplo: `23775072` o `/consultar V23775072`",
        parse_mode="MarkdownV2",
    )


async def mensaje_directo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw = update.message.text.strip().upper()
    if raw.isdigit() or (len(raw) > 1 and raw[0] in ("V", "E") and raw[1:].isdigit()):
        await procesar_cedula_raw(update, context, raw)
    else:
        await update.message.reply_text(
            "🤔 No entendí ese mensaje\\.\n\n"
            "Envíame solo el número de cédula o usa /consultar\n"
            "Ejemplo: `23775072`",
            parse_mode="MarkdownV2",
        )


async def registrar_mensaje_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Guarda texto entrante (privados y grupos) para poder exportar PDF después."""
    import chat_export_sqlite

    if not user_has_access(update):
        return
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message
    if chat is None or user is None or msg is None:
        return
    if user.is_bot:
        return
    if chat.type not in ("private", "group", "supergroup"):
        return
    text = msg.text or ""
    if not text.strip():
        return
    disp = user.full_name or " ".join(
        x for x in (user.first_name or "", user.last_name or "") if x
    ).strip()
    kind = "cmd" if text.strip().startswith("/") else "msg"
    chat_export_sqlite.append_line(
        chat_id=chat.id,
        user_id=user.id,
        username=user.username,
        display_name=disp or None,
        body=text,
        kind=kind,
    )


async def exportar_chat_pdf_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Genera un PDF con los mensajes de texto registrados para este chat."""
    import chat_export_sqlite

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return
    chat = update.effective_chat
    if chat is None:
        return
    chat_id = chat.id
    title = None
    if chat.type in ("group", "supergroup"):
        title = chat.title
    elif update.effective_user:
        title = update.effective_user.full_name or str(update.effective_user.id)

    wait = await msg.reply_text("📄 Generando PDF del historial registrado…")

    def _pdf() -> tuple[bytes, str]:
        return chat_export_sqlite.build_pdf(chat_id, chat_title=title)

    def _txt() -> tuple[bytes, str]:
        return chat_export_sqlite.build_txt(chat_id, chat_title=title)

    try:
        data, fname = await asyncio.to_thread(_pdf)
    except ValueError:
        await wait.edit_text(
            "No hay mensajes guardados para este chat.\n\n"
            "Solo se registran mensajes de texto mientras el bot está activo."
        )
        return
    except Exception as e:
        logger.warning("Export PDF falló (%s), intento TXT: %s", type(e).__name__, e)
        try:
            data, fname = await asyncio.to_thread(_txt)
        except ValueError:
            await wait.edit_text(
                "No hay mensajes guardados para este chat.\n\n"
                "Solo se registran mensajes de texto mientras el bot está activo."
            )
            return
        except Exception as e2:
            logger.exception("Export TXT falló: %s", e2)
            await wait.edit_text(
                "❌ No se pudo generar el archivo. Revisa los logs del servidor."
            )
            return
        await wait.delete()
        await msg.reply_document(
            document=BytesIO(data),
            filename=fname,
            caption="Historial en texto plano (fallback).",
        )
        return

    await wait.delete()
    cap = (
        "Historial exportado (mensajes de texto registrados en el servidor). "
        "No incluye conversaciones anteriores a usar el bot."
    )
    if len(data) > 49 * 1024 * 1024:
        await msg.reply_text(
            "❌ El archivo supera el límite de Telegram (~50 MB). "
            "Borra parte del historial con /olvidar_historial o reduce CHAT_EXPORT_MAX_LINES."
        )
        return
    await msg.reply_document(document=BytesIO(data), filename=fname, caption=cap)


async def olvidar_historial_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    import chat_export_sqlite

    msg = update.effective_message
    if not msg:
        return
    if not await ensure_user_allowed(update):
        return
    chat = update.effective_chat
    if chat is None:
        return

    n = await asyncio.to_thread(chat_export_sqlite.clear_chat, chat.id)
    await msg.reply_text(f"🗑️ Se eliminaron {n} líneas del historial guardado en el servidor.")


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
        load_dotenv("VARIABLES_BOT.env.txt")
    except ImportError:
        pass

    allowed_ids = _parse_allowed_telegram_user_ids()
    env_raw = os.environ.get("TELEGRAM_ALLOWED_USER_IDS")
    logger.info(
        "Lista blanca: env TELEGRAM_ALLOWED_USER_IDS=%r → IDs parseados: %s",
        env_raw,
        sorted(allowed_ids) if allowed_ids else "(ninguno: bot abierto)",
    )

    print("🤖 Iniciando Bot de Cédulas Venezolanas...")
    if allowed_ids:
        print(f"🔒 Acceso limitado a {len(allowed_ids)} usuario(s) de Telegram.")
    else:
        print("⚠️  Sin TELEGRAM_ALLOWED_USER_IDS: cualquier usuario puede usar el bot.")
        logger.warning(
            "Lista blanca vacía. En Render, añade TELEGRAM_ALLOWED_USER_IDS al servicio Web "
            "y redeploy. Sin eso, la variable no llega al proceso."
        )

    uf = filters.User(user_id=list(allowed_ids)) if allowed_ids else filters.ALL
    deny = (~filters.User(user_id=list(allowed_ids))) if allowed_ids else None

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    try:
        import chat_export_sqlite

        chat_export_sqlite.init_db()
    except Exception as e:
        logger.warning("chat_export_sqlite.init_db: %s", e)

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("consultar", consultar_command, filters=uf)],
        states={
            ESPERANDO_CEDULA: [
                CallbackQueryHandler(nacionalidad_callback, pattern="^NAC_"),
                MessageHandler((filters.TEXT & ~filters.COMMAND) & uf, recibir_cedula),
            ],
        },
        fallbacks=[CommandHandler("start", start, filters=uf)],
    )

    app.add_handler(CommandHandler("start", start, filters=uf))
    app.add_handler(CommandHandler("help", help_command, filters=uf))
    app.add_handler(CommandHandler("digitel", digitel_command, filters=uf))
    app.add_handler(CommandHandler("gnb", gnb_command, filters=uf))
    app.add_handler(CommandHandler("cicpc", cicpc_command, filters=uf))
    app.add_handler(CommandHandler("pnb", pnb_command, filters=uf))
    app.add_handler(CommandHandler("seniat", seniat_command, filters=uf))
    app.add_handler(CommandHandler("exportar_chat", exportar_chat_pdf_command, filters=uf))
    app.add_handler(CommandHandler("olvidar_historial", olvidar_historial_command, filters=uf))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(nueva_consulta_callback, pattern="^NUEVA_CONSULTA$"))
    app.add_handler(MessageHandler((filters.TEXT & ~filters.COMMAND) & uf, mensaje_directo))
    app.add_handler(MessageHandler(filters.TEXT & uf, registrar_mensaje_chat), group=-1)
    if deny is not None:
        app.add_handler(MessageHandler(filters.COMMAND & deny, access_denied_reply))
        app.add_handler(
            MessageHandler((filters.TEXT & ~filters.COMMAND) & deny, access_denied_reply)
        )

    if WEBHOOK_URL:
        # ── Modo PRODUCCIÓN (Koyeb / Render / Railway) ──────────────
        print(f"🌐 Modo WEBHOOK activo → {WEBHOOK_URL}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TELEGRAM_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{TELEGRAM_TOKEN}",
        )
    else:
        # ── Modo LOCAL (tu PC) ──────────────────────────────────────
        print("💻 Modo POLLING activo (desarrollo local)")
        app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
