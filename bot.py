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
import logging
import requests
from bs4 import BeautifulSoup
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
# ════════════════════════════════════════════════════════

API_URL  = "https://api.cedula.com.ve/api/v1"

# IVSS - Constancia de Cotizaciones
IVSS_URL = "http://www.ivss.gob.ve:28088/ConstanciaCotizacion/BuscaCotizacionCTRL"

# INTT - Consulta de Vehículos (Laravel Livewire)
INTT_BASE_URL  = "http://consulta.intt.gob.ve"
INTT_LOGIN_URL = f"{INTT_BASE_URL}/ingreso"
INTT_UPDATE_URL = f"{INTT_BASE_URL}/livewire/update"
INTT_VEH_URL   = f"{INTT_BASE_URL}/consulta-vehiculos"

INTT_USER      = os.environ.get("INTT_USER", "Ee30743649@gmail.com")
INTT_PASS      = os.environ.get("INTT_PASS", "30743649Ee.")

ESPERANDO_CEDULA = 1

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Consulta a la API de cedula.com.ve
# ─────────────────────────────────────────────
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
        return resp.json()
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


def formatear_respuesta_intt(data: dict, nac: str, ced: str) -> str:
    """Formatea los resultados del INTT."""
    def esc(v: str) -> str:
        if not v: return "—"
        v = str(v)
        for ch in r"_*[]()~`>#+-=|{}.!\\":
            v = v.replace(ch, f"\\{ch}")
        return v

    lin = []
    lin.append("╔══════════════════════════╗")
    lin.append("║  🚗  DATOS INTT (Vehículos) ║")
    lin.append("╚══════════════════════════╝")
    lin.append("")
    lin.append(f"🪪  *Cédula:*  `{nac}\\-{ced}`")
    lin.append("")

    owner = data.get("owner", {})
    if owner:
        lin.append("👤 *PROPIETARIO:*")
        lin.append(f"   • Nombre: `{esc(owner.get('nombre', '—'))}`")
        if owner.get("telefono") and owner["telefono"] != "No disponible":
            lin.append(f"   • Teléfono: `{esc(owner['telefono'])}`")
        if owner.get("sangre") and owner["sangre"] != "No disponible":
            lin.append(f"   • Sangre: `{esc(owner['sangre'])}`")
        if owner.get("direccion") and owner["direccion"] != "No disponible":
            lin.append(f"   • Dirección: `{esc(owner['direccion'])}`")
        lin.append("")

    vehicles = data.get("vehicles", [])
    if not vehicles:
        lin.append("❌ *No se encontraron vehículos registrados\\.*")
    else:
        for i, veh in enumerate(vehicles, 1):
            lin.append(f"🚘 *Vehículo #{i}:*")
            lin.append(f"   📟 Placa: `{esc(veh.get('placa'))}`")
            lin.append(f"   🏢 Marca: `{esc(veh.get('marca'))}`")
            lin.append(f"   🚗 Modelo: `{esc(veh.get('modelo'))}`")
            lin.append(f"   🎨 Color: `{esc(veh.get('color'))}`")
            lin.append(f"   📅 Año: `{esc(veh.get('año'))}`")
            lin.append(f"   🔖 Estado: `{esc(veh.get('estado'))}`")
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
        "╔══════════════════════════╗\n"
        "║  📋  DATOS ENCONTRADOS   ║\n"
        "╚══════════════════════════╝\n\n"
        f"🪪  *Cédula:*          `{nac}-{ced}`\n"
        f"🧾  *R\\.I\\.F\\.:*         `{rif}`\n"
        f"👤  *Nombre:*          `{nombre}`\n\n"
        "🗳️  *Datos CNE*\n"
        f"    📍 Estado:          `{estado}`\n"
        f"    🏘️  Municipio:      `{municipio}`\n"
        f"    ⛪ Parroquia:       `{parroquia}`\n"
        f"    🏫 Centro Electoral:\n"
        f"       `{centro}`\n"
    )


def formatear_respuesta_ivss(data: dict, nac: str, ced: str) -> str:
    """Convierte el diccionario parseado del IVSS en texto Markdown para Telegram."""
    def esc(v: str) -> str:
        if not v: return ""
        v = str(v)
        for ch in r"_*[]()~`>#+-=|{}.!\\":
            v = v.replace(ch, f"\\{ch}")
        return v

    lin = []
    lin.append("╔══════════════════════════╗")
    lin.append("║  🏥  DATOS IVSS          ║")
    lin.append("╚══════════════════════════╝")
    lin.append("")
    lin.append(f"🪪  *Cédula:*  `{nac}\\-{ced}`")
    lin.append("")

    claves_emoji = {
        "semanas cotizadas":   "📊",
        "afiliacion":          "📅",
        "afiliación":          "📅",
        "fecha de afiliacion": "📅",
        "fecha de afiliación": "📅",
        "estatus":             "🔖",
        "status":              "🔖",
        "empresa":             "🏢",
        "empleador":           "🏢",
        "patronal":            "🔢",
        "numero patronal":     "🔢",
        "número patronal":     "🔢",
        "egreso":              "📤",
        "fecha de egreso":     "📤",
        "vigencia":            "⏳",
    }

    for key, val in data.items():
        if not val:
            continue
        key_lower = key.lower()
        emoji = next(
            (v for k, v in claves_emoji.items() if k in key_lower),
            "▪️"
        )
        lin.append(f"{emoji}  *{esc(key)}:*")
        lin.append(f"    `{esc(val)}`")

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
        "  • Mostrar nombre, RIF y estado donde vota\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📲 *Comandos disponibles:*\n"
        "  /consultar — Iniciar una consulta\n"
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
        "ℹ️ Datos que obtienes:\n"
        "  • Nombre completo\n"
        "  • R\\.I\\.F\\.\n"
        "  • Estado donde vota\n"
        "  • Municipio, Parroquia y Centro Electoral\n"
    )
    await update.message.reply_text(texto, parse_mode="MarkdownV2")


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
        "🔍 Consultando *múltiples fuentes* en paralelo\\.\\.\\. un momento ⏳",
        parse_mode="MarkdownV2",
    )

    # Consultar las 3 APIs al mismo tiempo
    loop = asyncio.get_event_loop()
    result_cedula, result_ivss, result_intt = await asyncio.gather(
        loop.run_in_executor(None, consultar_cedula, cedula, nacionalidad),
        loop.run_in_executor(None, consultar_ivss,   cedula, nacionalidad),
        loop.run_in_executor(None, consultar_intt,   cedula, nacionalidad),
    )

    # ── Bloque 1: Cédula / CNE ─────────────────────────────────────────
    if result_cedula.get("error"):
        error = result_cedula.get("error_str", "Error desconocido.")
        await msg.edit_text(f"❌ *CNE:* `{error}`", parse_mode="MarkdownV2")
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
        await update.message.reply_text(f"⚠️ *IVSS:* {error_ivss}", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text(
            formatear_respuesta_ivss(result_ivss.get("data", {}), nacionalidad, cedula),
            parse_mode="MarkdownV2",
        )

    # ── Bloque 3: INTT (Vehículos) ─────────────────────────────────────
    try:
        if result_intt.get("error"):
            error_intt = result_intt.get("error_str", "Error desconocido.")
            await update.message.reply_text(f"⚠️ *INTT:* {error_intt}", parse_mode="MarkdownV2")
        else:
            txt_intt = formatear_respuesta_intt(result_intt, nacionalidad, cedula)
            await update.message.reply_text(txt_intt, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error enviando mensaje INTT: {e}")
        await update.message.reply_text("❌ *INTT:* El mensaje contiene caracteres no compatibles o hubo un fallo al enviarlo\\.", parse_mode="MarkdownV2")

    logger.info("Consulta completa: %s-%s", nacionalidad, cedula)


async def nueva_consulta_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
def main() -> None:
    print("🤖 Iniciando Bot de Cédulas Venezolanas...")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("consultar", consultar_command)],
        states={
            ESPERANDO_CEDULA: [
                CallbackQueryHandler(nacionalidad_callback, pattern="^NAC_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, recibir_cedula),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(nueva_consulta_callback, pattern="^NUEVA_CONSULTA$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, mensaje_directo))

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
