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
import io
import json
import asyncio
import logging
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup

try:
    from PIL import Image, ImageFilter, ImageOps
    import pytesseract
    OCR_DISPONIBLE = True
except ImportError:
    OCR_DISPONIBLE = False

try:
    import fitz  # PyMuPDF
    PDF_DISPONIBLE = True
except ImportError:
    PDF_DISPONIBLE = False

# Tamaño máximo del extracto de planilla por mensaje (Telegram ~4096 con formato)
SENIAT_PLANILLA_MAX_CHARS = 3200
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

# SENIAT - Consulta de contribuyentes
SENIAT_BASE    = "http://contribuyente.seniat.gob.ve"
SENIAT_LOGIN   = f"{SENIAT_BASE}/iseniatlogin/contribuyente.do"
SENIAT_CAPTCHA = f"{SENIAT_BASE}/iseniatlogin/kapatcha/kapatch.jpg"
# Consulta RUI / contribuyentes activos (tras sesión iSENIAT)
SENIAT_RIFCONSULTA_LOGIN = f"{SENIAT_BASE}/rifconsulta/login.do"
SENIAT_PAGINA_PRINCIPAL  = f"{SENIAT_BASE}/iseniatlogin/paginaprincipal.do"

SENIAT_USER    = os.environ.get("SENIAT_USER", "V20878510")
SENIAT_PASS    = os.environ.get("SENIAT_PASS", "v20878510")

# Endpoints de consulta (fallback si el menú post-login no expone enlaces claros)
SENIAT_CONSULTA_FALLBACK = [
    f"{SENIAT_BASE}/iseniat/contribuyente/ConsultaContribuyente.do",
    f"{SENIAT_BASE}/iseniat/contribuyente/BuscarContribuyente.do",
    f"{SENIAT_BASE}/iseniat/jsp/contribuyente/consultaContribuyente.jsp",
    f"{SENIAT_BASE}/contribuyente/ConsultaContribuyente.do",
    f"{SENIAT_BASE}/contribuyente/BuscarContribuyente.do",
    f"{SENIAT_BASE}/islr/contribuyente/BuscarContribuyente.do",
]

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


# ─────────────────────────────────────────────
#  SENIAT - OCR + Consulta
# ─────────────────────────────────────────────
def _seniat_parse_login_form(soup, page_url: str):
    """El POST real va a `login.do` (action del form), no a contribuyente.do."""
    form = (
        soup.find("form", id="consultaForm")
        or soup.find("form", attrs={"name": "DatosLoginForm"})
        or soup.find("form")
    )
    if not form:
        return None, {}
    action = (form.get("action") or "").strip() or "/iseniatlogin/login.do"
    post_url = urljoin(page_url, action)
    hidden = {}
    for inp in form.find_all("input", {"type": "hidden"}):
        name = inp.get("name")
        if name:
            hidden[name] = inp.get("value") or ""
    return post_url, hidden


def _seniat_sigue_en_login(html: str) -> bool:
    """True si el servidor devolvió de nuevo la pantalla de acceso (captcha/usuario)."""
    if not html:
        return True
    if 'id="consultaForm"' in html or "id='consultaForm'" in html:
        return True
    if 'name="kaptcha"' in html or "name='kaptcha'" in html:
        return True
    return False


def _seniat_variantes_documento(nacionalidad: str, cedula: str) -> list[str]:
    """RIF/cédula en formatos que suelen aceptar los formularios del portal."""
    nac = (nacionalidad or "V").strip().upper()[:1]
    ced = re.sub(r"\D", "", str(cedula or ""))
    if not ced:
        return []
    candidatos = [
        f"{nac}{ced}",
        f"{nac}{ced.zfill(8)}",
        f"{nac}{ced.zfill(9)}",
        ced,
        ced.zfill(8),
    ]
    vistos, ordenados = set(), []
    for c in candidatos:
        if c and c not in vistos:
            vistos.add(c)
            ordenados.append(c)
    return ordenados


def _seniat_extraer_urls_consulta(html: str, base_url: str) -> list[str]:
    """Enlaces .do/.jsp del menú post-login (consulta de contribuyente, RIF, etc.)."""
    skip = (
        "javascript:", "logout", "cerrar", "salir", "olvido", "recclave",
        "registronat", "kapatcha/", "/iseniatlogin/login.do", "window.close",
    )
    keys = (
        "consulta", "buscar", "contribuyente", "rif", "datos", "ciudadano",
        "constancia", "misdat", "persona", "natural", "activida", "domicilio",
    )
    encontrados, orden = set(), []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["a", "area"], href=True):
        href = tag["href"].strip()
        low = href.lower()
        if any(s in low for s in skip):
            continue
        if ".do" not in low and ".jsp" not in low:
            continue
        if not any(k in low for k in keys):
            continue
        full = urljoin(base_url, href)
        if full not in encontrados:
            encontrados.add(full)
            orden.append(full)
    for tag in soup.find_all(["frame", "iframe"], src=True):
        href = tag.get("src", "").strip()
        low = href.lower()
        if not href or href.lower().startswith("javascript:"):
            continue
        if any(s in low for s in skip):
            continue
        if ".do" not in low and ".jsp" not in low:
            continue
        full = urljoin(base_url, href)
        if full not in encontrados:
            encontrados.add(full)
            orden.append(full)
    for tag in soup.find_all("form", action=True):
        act = tag.get("action", "").strip()
        low = act.lower()
        if not act or any(s in low for s in skip):
            continue
        if ".do" not in low and ".jsp" not in low:
            continue
        if not any(k in low for k in keys):
            continue
        full = urljoin(base_url, act)
        if full not in encontrados:
            encontrados.add(full)
            orden.append(full)
    return orden


def _seniat_respuesta_tiene_datos(texto_plano: str) -> bool:
    t = texto_plano.upper()
    return any(
        kw in t
        for kw in (
            "RIF", "NOMBRE", "RAZÓN", "RAZON", "CONTRIBUYENTE", "DOMICILIO", "ACTIVIDAD",
            "BÚSQUEDA", "BUSQUEDA", "CONTRIBUYENTES ACTIVOS", "REGISTRO ÚNICO",
        )
    )


def _seniat_rifconsulta_pagina_error(html: str) -> bool:
    low = (html or "").lower()
    return (
        "operación inválida" in low
        or "operacion invalida" in low
        or "no es posible procesar" in low
    )


def _seniat_valor_cedula_rifconsulta(nac_u: str, ced_digits: str) -> str:
    """Portal: CI con letra V/E, sin guiones ni puntos (ej. V12345678)."""
    return f"{nac_u}{ced_digits}"


def _seniat_pdf_a_texto(blob: bytes) -> str:
    """Extrae texto plano de un PDF (planilla RIF)."""
    if not PDF_DISPONIBLE or not blob or not blob.startswith(b"%PDF"):
        return ""
    try:
        doc = fitz.open(stream=blob, filetype="pdf")
        partes = []
        for i in range(len(doc)):
            partes.append(doc[i].get_text())
        doc.close()
        return re.sub(r"\n{3,}", "\n\n", "\n".join(partes)).strip()
    except Exception as e:
        logger.warning(f"SENIAT PDF: no se pudo leer el PDF: {e}")
        return ""


def _seniat_parse_fila_listado_contribuyentes(soup) -> dict:
    """Tabla 'Listado de Contribuyentes' (RIF enlace, Nombres, Situación)."""
    for table in soup.find_all("table"):
        filas = table.find_all("tr")
        if len(filas) < 2:
            continue
        cab = " ".join(c.get_text(" ", strip=True).lower() for c in filas[0].find_all(["th", "td"]))
        if "rif" not in cab or "nombre" not in cab:
            continue
        for fila in filas[1:]:
            celdas = fila.find_all("td")
            if len(celdas) < 3:
                continue
            a = celdas[0].find("a", href=True)
            rif_t = (a.get_text(strip=True) if a else celdas[0].get_text(strip=True)) or ""
            nombres = celdas[1].get_text(" ", strip=True)
            situacion = celdas[2].get_text(" ", strip=True)
            if rif_t and re.match(r"^[VEJPG]\d{5,14}$", rif_t, re.I):
                d = {}
                if rif_t:
                    d["RIF"] = rif_t
                if nombres:
                    d["Nombres"] = nombres
                if situacion:
                    d["Situación"] = situacion
                return d
    return {}


def _seniat_listado_enlace_ficha_rif(soup, base_url: str, rif_busqueda: str) -> str | None:
    """URL del enlace azul bajo la columna RIF (primera fila que case con la cédula buscada)."""
    norm = re.sub(r"\D", "", rif_busqueda or "")
    pref = (rif_busqueda or "V")[:1].upper()
    candidatos: list[tuple[str, str]] = []
    for table in soup.find_all("table"):
        for a in table.find_all("a", href=True):
            label = (a.get_text(strip=True) or "").upper().replace(" ", "")
            if not re.match(r"^[VEJPG]\d{5,14}$", label, re.I):
                continue
            href = a["href"].strip()
            if href.lower().startswith("javascript:"):
                continue
            full = urljoin(base_url, href)
            dig = re.sub(r"\D", "", label)
            candidatos.append((full, dig))
    for full, dig in candidatos:
        if norm and (dig.startswith(norm) or norm in dig or dig[: len(norm)] == norm[: len(dig)]):
            return full
    for full, dig in candidatos:
        if dig.startswith(pref):
            return full
    return candidatos[0][0] if candidatos else None


def _seniat_enlace_ver_planilla(soup, page_url: str) -> str | None:
    """Menú lateral: enlace 'Ver Planilla'."""
    for a in soup.find_all("a", href=True):
        texto = a.get_text(" ", strip=True).lower()
        href = a["href"].strip()
        if "javascript:" in href.lower():
            continue
        if "ver planilla" in texto or (texto == "planilla" and "ver" in (a.get("title") or "").lower()):
            return urljoin(page_url, href)
        low = href.lower()
        if "planilla" in low and (".do" in low or ".jsp" in low):
            return urljoin(page_url, href)
    return None


def _seniat_descargar_texto_planilla(
    session, url: str, headers: dict, referer: str, depth: int = 0
) -> str | None:
    """GET de la planilla: PDF directo o HTML con iframe/enlace al PDF."""
    if depth > 6:
        return None
    hdr = {**headers, "Referer": referer, "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8"}
    try:
        r = session.get(url, headers=hdr, timeout=45, allow_redirects=True)
    except Exception as e:
        logger.warning(f"SENIAT planilla GET: {e}")
        return None
    data = r.content
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "pdf" in ctype or (data[:5] if len(data) > 5 else b"").startswith(b"%PDF"):
        txt = _seniat_pdf_a_texto(data)
        return txt if txt else None
    if "html" not in ctype and len(data) > 100:
        txt = _seniat_pdf_a_texto(data)
        if txt:
            return txt
    soup = BeautifulSoup(r.text or "", "html.parser")
    for tag in soup.find_all(["iframe", "frame", "embed"], src=True):
        src = (tag.get("src") or "").strip()
        if not src or "javascript:" in src.lower():
            continue
        sub = urljoin(r.url, src)
        if ".pdf" in sub.lower() or "pdf" in sub.lower() or "planilla" in sub.lower():
            t = _seniat_descargar_texto_planilla(session, sub, headers, r.url, depth + 1)
            if t:
                return t
    for a in soup.find_all("a", href=True):
        h = a["href"].strip()
        if ".pdf" in h.lower() or "planilla" in h.lower():
            t = _seniat_descargar_texto_planilla(session, urljoin(r.url, h), headers, r.url, depth + 1)
            if t:
                return t
    return None


def _seniat_parse_form_busqueda_rifconsulta(soup, page_url: str):
    """
    Formulario 'Búsqueda de Contribuyentes Activos' en rifconsulta.
    Retorna (post_url, payload_inicial, nombre_campo_cedula, submit_name, submit_value) o Nones.
    """
    for form in soup.find_all("form", method=re.compile("post", re.I)):
        blob = form.get_text(" ").lower()
        if not any(
            x in blob
            for x in ("cédula", "cedula", "pasaporte", "contribuyente", "rif", "apellido", "razón", "razon")
        ):
            continue
        action = (form.get("action") or "").strip() or "/rifconsulta/login.do"
        post_url = urljoin(page_url, action)
        payload = {}
        for inp in form.find_all("input", {"type": "hidden"}):
            n = inp.get("name")
            if n:
                payload[n] = inp.get("value") or ""
        ced_name = None
        for tr in form.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            label = tds[0].get_text(" ", strip=True).lower()
            if ("cédula" in label or "cedula" in label or "pasaporte" in label) and (
                "apellido" not in label and "razón" not in label and "razon" not in label
            ):
                inp = None
                for td in tds[1:]:
                    inp = td.find("input", attrs={"type": re.compile(r"text|search", re.I)})
                    if inp:
                        break
                if inp and inp.get("name"):
                    ced_name = inp["name"]
                    break
        if not ced_name:
            for guess in (
                "cedulaPasaporte",
                "cedula",
                "cedulaPasaport",
                "documento",
                "numDocumento",
                "pCedula",
                "cit",
                "ci",
                "documentoIdentidad",
            ):
                el = form.find("input", attrs={"name": guess})
                if el:
                    ced_name = guess
                    break
        if not ced_name:
            continue
        submit_name, submit_val = None, None
        for inp in form.find_all("input", attrs={"type": re.compile(r"submit|image", re.I)}):
            submit_name = inp.get("name")
            submit_val = inp.get("value") or ""
            if "buscar" in submit_val.lower():
                break
        return post_url, payload, ced_name, submit_name, submit_val
    return None, None, None, None, None


def _seniat_rifconsulta_buscar_contribuyente(
    session,
    headers: dict,
    r_post_login,
    nacionalidad: str,
    cedula: str,
    rif_busqueda: str,
) -> dict | None:
    """
    Tras login iSENIAT: rifconsulta → Buscar por cédula → enlace RIF → Ver Planilla → texto PDF.
    Retorna {"data": dict, "planilla": str|None} o None si no aplica / falla del todo.
    """
    menu_base = r_post_login.url
    hdr = {**headers, "Referer": menu_base}
    try:
        r0 = session.get(
            SENIAT_RIFCONSULTA_LOGIN,
            headers=hdr,
            timeout=22,
            allow_redirects=True,
        )
        if _seniat_rifconsulta_pagina_error(r0.text):
            logger.info("SENIAT: rifconsulta requiere paso por página principal; reintentando...")
            session.get(
                SENIAT_PAGINA_PRINCIPAL,
                headers={**headers, "Referer": menu_base},
                timeout=18,
                allow_redirects=True,
            )
            r0 = session.get(
                SENIAT_RIFCONSULTA_LOGIN,
                headers={**headers, "Referer": SENIAT_PAGINA_PRINCIPAL},
                timeout=22,
                allow_redirects=True,
            )
        if _seniat_rifconsulta_pagina_error(r0.text):
            logger.warning("SENIAT: rifconsulta devolvió error de portal (sin formulario).")
            return None

        soup = BeautifulSoup(r0.text, "html.parser")
        post_url, base_payload, ced_name, submit_name, submit_val = _seniat_parse_form_busqueda_rifconsulta(
            soup, r0.url
        )
        if not post_url or not ced_name:
            logger.warning("SENIAT: no se detectó el formulario de búsqueda en rifconsulta/login.do")
            return None

        nac_u = (nacionalidad or "V").strip().upper()[:1]
        ced_digits = re.sub(r"\D", "", str(cedula or ""))
        valor_ci = _seniat_valor_cedula_rifconsulta(nac_u, ced_digits)

        payload = dict(base_payload)
        for inp in soup.find_all("input"):
            itype = (inp.get("type") or "text").lower()
            name = inp.get("name")
            if not name or itype == "hidden" or itype == "submit" or itype == "image":
                continue
            if itype in ("text", "search") and name != ced_name and name not in payload:
                payload[name] = ""
        payload[ced_name] = valor_ci
        if submit_name:
            payload[submit_name] = submit_val or ""

        r1 = session.post(
            post_url,
            data=payload,
            headers={
                **headers,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": r0.url,
            },
            timeout=25,
            allow_redirects=True,
        )
        if r1.status_code != 200 or len(r1.text) < 400:
            return None
        if _seniat_sigue_en_login(r1.text):
            return None
        soup1 = BeautifulSoup(r1.text, "html.parser")
        texto = soup1.get_text(separator=" ", strip=True)
        if not _seniat_respuesta_tiene_datos(texto):
            return None

        data = _parsear_datos_seniat(soup1, texto, rif_busqueda) or {}
        listado = _seniat_parse_fila_listado_contribuyentes(soup1)
        for k, v in listado.items():
            if v and (k not in data or not data.get(k)):
                data[k] = v

        planilla_txt: str | None = None
        rif_href = _seniat_listado_enlace_ficha_rif(soup1, r1.url, rif_busqueda)
        if rif_href:
            logger.info("SENIAT: abriendo ficha desde enlace RIF del listado...")
            r2 = session.get(
                rif_href,
                headers={**headers, "Referer": r1.url},
                timeout=28,
                allow_redirects=True,
            )
            if r2.status_code == 200 and len(r2.text) > 300 and not _seniat_sigue_en_login(r2.text):
                soup2 = BeautifulSoup(r2.text, "html.parser")
                t2 = soup2.get_text(separator=" ", strip=True)
                data2 = _parsear_datos_seniat(soup2, t2, rif_busqueda) or {}
                for k, v in data2.items():
                    if v and (k not in data or len(str(v)) > len(str(data.get(k, "")))):
                        data[k] = v
                plan_url = _seniat_enlace_ver_planilla(soup2, r2.url)
                if plan_url:
                    logger.info("SENIAT: descargando planilla PDF (Ver Planilla)...")
                    planilla_txt = _seniat_descargar_texto_planilla(
                        session, plan_url, headers, r2.url
                    )

        if not data and not planilla_txt:
            return None
        if not data:
            data = {"Nota": "Solo se obtuvo texto de planilla PDF"}
        return {"data": data, "planilla": planilla_txt}
    except Exception as e:
        logger.warning(f"SENIAT rifconsulta: {e}")
        return None


def _ocr_captcha(img_bytes: bytes) -> str:
    """Aplica preprocesamiento a la imagen del captcha y extrae el texto con Tesseract."""
    if not OCR_DISPONIBLE:
        return ""
    try:
        img = Image.open(io.BytesIO(img_bytes)).convert("L")  # Escala de grises
        # Escalar x3 para mejorar reconocimiento
        w, h = img.size
        img = img.resize((w * 3, h * 3), Image.LANCZOS)
        # Umbral: pixeles < 140 → negro, resto → blanco
        img = img.point(lambda px: 0 if px < 140 else 255, "1")
        # Suavizado leve para eliminar ruido
        img = img.filter(ImageFilter.MedianFilter(size=3))
        # OCR con solo letras y dígitos, sin espacios
        texto = pytesseract.image_to_string(
            img,
            config="--psm 8 --oem 3 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        )
        return texto.strip().replace(" ", "")
    except Exception as e:
        logger.warning(f"SENIAT OCR error: {e}")
        return ""


def consultar_seniat(cedula: str, nacionalidad: str = "V") -> dict:
    """Inicia sesión en el portal del SENIAT, resuelve el CAPTCHA con OCR y consulta los datos."""
    if not OCR_DISPONIBLE:
        return {"error": True, "error_str": "⚙️ OCR no disponible. Instala Pillow y pytesseract."}

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": SENIAT_LOGIN,
        "Accept-Language": "es-VE,es;q=0.9",
    }

    MAX_INTENTOS_CAPTCHA = 4
    intento = 0
    r_post_login = None

    try:
        # ── Login: GET login siempre fresco + POST a action real (login.do) ──
        for intento in range(1, MAX_INTENTOS_CAPTCHA + 1):
            logger.info(f"SENIAT: Cargando login (intento {intento}/{MAX_INTENTOS_CAPTCHA})...")
            r_login = session.get(SENIAT_LOGIN, headers=headers, timeout=22)
            r_login.raise_for_status()
            soup_login = BeautifulSoup(r_login.text, "html.parser")
            post_url, hidden_fields = _seniat_parse_login_form(soup_login, r_login.url)
            if not post_url:
                return {"error": True, "error_str": "❌ Formulario de login del SENIAT no reconocido."}

            logger.info("SENIAT: Descargando captcha...")
            hdr_cap = {**headers, "Referer": r_login.url}
            r_cap = session.get(SENIAT_CAPTCHA, headers=hdr_cap, timeout=18)
            r_cap.raise_for_status()
            captcha_texto = _ocr_captcha(r_cap.content)

            if not captcha_texto:
                logger.warning(f"SENIAT: OCR vacío en intento {intento}.")
                continue

            logger.info(f"SENIAT: OCR captcha → '{captcha_texto}'")

            payload = {
                **hidden_fields,
                "usuario": SENIAT_USER,
                "clave": SENIAT_PASS,
                "kaptcha": captcha_texto,
            }
            r2 = session.post(
                post_url,
                data=payload,
                headers={
                    **headers,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Referer": r_login.url,
                },
                timeout=22,
                allow_redirects=True,
            )

            if _seniat_sigue_en_login(r2.text):
                logger.warning(f"SENIAT: Credenciales o CAPTCHA no aceptados (intento {intento}).")
                continue

            r_post_login = r2
            logger.info("SENIAT: Login exitoso.")
            break

        if r_post_login is None:
            return {
                "error": True,
                "error_str": f"🔒 No se pudo iniciar sesión en el SENIAT tras {MAX_INTENTOS_CAPTCHA} intentos.",
            }

        ced_digits = re.sub(r"\D", "", str(cedula or ""))
        nac_u = (nacionalidad or "V").strip().upper()[:1]
        rif_busqueda = f"{nac_u}{ced_digits}"
        variantes_doc = _seniat_variantes_documento(nacionalidad, cedula)
        planilla_texto = None

        logger.info("SENIAT: Consulta rifconsulta (Cédula / Pasaporte)...")
        rif_out = _seniat_rifconsulta_buscar_contribuyente(
            session, headers, r_post_login, nacionalidad, cedula, rif_busqueda
        )
        data_contribuyente = None
        if rif_out:
            data_contribuyente = rif_out.get("data") or {}
            planilla_texto = rif_out.get("planilla")

        if not data_contribuyente:
            # ── Respaldo: menú iSENIAT + otros endpoints ──
            html_menu = r_post_login.text
            menu_base = r_post_login.url
            s_menu = BeautifulSoup(html_menu, "html.parser")
            for tag in s_menu.find_all(["frame", "iframe"], src=True):
                src = (tag.get("src") or "").strip()
                if not src or src.lower().startswith("javascript:"):
                    continue
                if ".do" not in src.lower() and ".jsp" not in src.lower():
                    continue
                fu = urljoin(menu_base, src)
                try:
                    rf = session.get(fu, headers={**headers, "Referer": menu_base}, timeout=18, allow_redirects=True)
                    if rf.status_code == 200 and len(rf.text) > 400:
                        html_menu += "\n" + rf.text
                except Exception:
                    pass

            endpoints_consulta = []
            vistos = set()
            for u in _seniat_extraer_urls_consulta(html_menu, menu_base):
                if u not in vistos:
                    vistos.add(u)
                    endpoints_consulta.append(u)
            for u in SENIAT_CONSULTA_FALLBACK:
                if u not in vistos:
                    vistos.add(u)
                    endpoints_consulta.append(u)

            nombres_rif = ("rif", "numRif", "numeroRif", "nroRif", "p_rif", "rifContribuyente")
            nombres_ced = ("cedula", "p_cedula", "documento", "numDocumento", "identificacion")

            def _intentar_url(url_consulta: str):
                hdr = {**headers, "Referer": menu_base}
                for doc in variantes_doc[:6]:
                    for pname in nombres_rif:
                        try:
                            r3 = session.get(
                                url_consulta,
                                params={pname: doc},
                                headers=hdr,
                                timeout=22,
                                allow_redirects=True,
                            )
                            if (
                                r3.status_code == 200
                                and len(r3.text) > 900
                                and "index.htm" not in (r3.url or "").lower()
                                and not _seniat_sigue_en_login(r3.text)
                            ):
                                soup3 = BeautifulSoup(r3.text, "html.parser")
                                page3_text = soup3.get_text(separator=" ", strip=True)
                                if _seniat_respuesta_tiene_datos(page3_text):
                                    data = _parsear_datos_seniat(soup3, page3_text, rif_busqueda)
                                    if data:
                                        return data
                        except Exception:
                            pass
                    base_data = {
                        "nacionalidad": (nacionalidad or "V").strip().upper()[:1],
                        "nac": (nacionalidad or "V").strip().upper()[:1],
                        "numero": ced_digits,
                    }
                    for pname in nombres_ced:
                        try:
                            pdata = {**base_data, pname: doc}
                            r3 = session.get(
                                url_consulta,
                                params=pdata,
                                headers=hdr,
                                timeout=22,
                                allow_redirects=True,
                            )
                            if (
                                r3.status_code == 200
                                and len(r3.text) > 900
                                and "index.htm" not in (r3.url or "").lower()
                                and not _seniat_sigue_en_login(r3.text)
                            ):
                                soup3 = BeautifulSoup(r3.text, "html.parser")
                                page3_text = soup3.get_text(separator=" ", strip=True)
                                if _seniat_respuesta_tiene_datos(page3_text):
                                    data = _parsear_datos_seniat(soup3, page3_text, rif_busqueda)
                                    if data:
                                        return data
                            pdata_post = {**base_data, pname: doc}
                            r4 = session.post(
                                url_consulta,
                                data=pdata_post,
                                headers={
                                    **hdr,
                                    "Content-Type": "application/x-www-form-urlencoded",
                                },
                                timeout=22,
                                allow_redirects=True,
                            )
                            if (
                                r4.status_code == 200
                                and len(r4.text) > 900
                                and "index.htm" not in (r4.url or "").lower()
                                and not _seniat_sigue_en_login(r4.text)
                            ):
                                soup4 = BeautifulSoup(r4.text, "html.parser")
                                page4_text = soup4.get_text(separator=" ", strip=True)
                                if _seniat_respuesta_tiene_datos(page4_text):
                                    data = _parsear_datos_seniat(soup4, page4_text, rif_busqueda)
                                    if data:
                                        return data
                        except Exception:
                            pass
                return None

            for url_consulta in endpoints_consulta[:12]:
                logger.info(f"SENIAT: Probando → {url_consulta}")
                data_contribuyente = _intentar_url(url_consulta)
                if data_contribuyente:
                    break

        if not data_contribuyente:
            soup2 = BeautifulSoup(r_post_login.text, "html.parser")
            page2_text = soup2.get_text(separator=" ", strip=True)
            if _seniat_respuesta_tiene_datos(page2_text):
                data_contribuyente = _parsear_datos_seniat(soup2, page2_text, rif_busqueda)

        if not data_contribuyente:
            return {
                "error": True,
                "error_str": "❌ Sesión SENIAT abierta, pero no se encontró la consulta de datos para esta cédula.",
            }

        logger.info(f"SENIAT: Consulta exitosa para {rif_busqueda}")
        return {
            "error": False,
            "data": data_contribuyente,
            "captcha_intentos": intento,
            "planilla_texto": planilla_texto,
        }

    except requests.exceptions.Timeout:
        return {"error": True, "error_str": "⏱️ El SENIAT tardó demasiado en responder."}
    except requests.exceptions.ConnectionError:
        return {"error": True, "error_str": "🔌 No se pudo conectar al portal del SENIAT."}
    except Exception as e:
        logger.error(f"SENIAT CRITICAL ERROR: {e}", exc_info=True)
        return {"error": True, "error_str": f"Error técnico en SENIAT: {str(e)}"}


def _parsear_datos_seniat(soup, texto: str, rif_busqueda: str) -> dict:
    """Extrae los datos del contribuyente del HTML del SENIAT."""
    data = {}

    # Intentar extraer de tablas
    tablas = soup.find_all("table")
    for tabla in tablas:
        filas = tabla.find_all("tr")
        for fila in filas:
            celdas = fila.find_all(["td", "th"])
            if len(celdas) >= 2:
                clave = re.sub(r"\s+", " ", celdas[0].get_text(strip=True)).strip(" :")
                valor = re.sub(r"\s+", " ", celdas[1].get_text(strip=True))
                if clave and valor and len(valor) > 1:
                    data[clave] = valor

    # Buscar con regex si la tabla falló
    patrones = [
        ("RIF",              r"(?:R\.?\s*I\.?\s*F\.?|RIF)\s*:?\s*([VEJPG][\s\-]?\d{6,12}(?:[\s\-]\d)?)"),
        ("Nombre / Razón",   r"(?:nombre|raz[oó]n social)\s*:?\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s,\.]{4,60})"),
        ("Domicilio Fiscal", r"(?:domicilio|direcci[oó]n)\s*:?\s*(.{10,120}?)(?:\n|,{2}|\s{3})"),
        ("Actividad",        r"actividad\s*:?\s*(.{5,80}?)(?:\n|\s{3})"),
        ("Estatus",          r"(?:estatus|estado)\s*:?\s*(ACTIVO|INACTIVO|SUSPENDIDO)"),
    ]
    for nombre_campo, patron in patrones:
        if nombre_campo not in data:
            m = re.search(patron, texto, re.IGNORECASE)
            if m:
                data[nombre_campo] = m.group(1).strip()

    if len(data) >= 2:
        return data
    if len(data) == 1:
        k, v = next(iter(data.items()))
        if k.upper().startswith("RIF") and rif_busqueda and rif_busqueda.upper().replace("-", "") in str(v).upper().replace("-", ""):
            return data
    return {}


def formatear_respuesta_seniat(
    data: dict,
    nac: str,
    ced: str,
    intentos: int = 1,
    planilla_texto: str | None = None,
) -> str:
    """Formatea los datos del SENIAT para Telegram MarkdownV2."""
    lin = []
    lin.append(escape_md("╔══════════════════════════╗"))
    lin.append(escape_md("║  🏛️  DATOS SENIAT        ║"))
    lin.append(escape_md("╚══════════════════════════╝"))
    lin.append("")
    lin.append(f"🪪  *{escape_md('Cédula:')}*  `{escape_md(nac, True)}-{escape_md(ced, True)}`")
    if intentos > 1:
        lin.append(f"🔐  _{escape_md(f'CAPTCHA resuelto en {intentos} intento(s)')}_")
    lin.append("")

    claves_emoji = {
        "rif":        "🧾",
        "nombre":     "👤", "razón":     "👤", "razon":    "👤",
        "domicilio":  "📍", "dirección": "📍", "direccion": "📍",
        "actividad":  "💼",
        "estatus":    "🔖", "estado":    "🔖", "status":    "🔖",
        "situación":  "🔖", "situacion": "🔖",
        "teléfono":   "📞", "telefono":  "📞",
        "correo":     "📧", "email":     "📧",
        "municipio":  "🏘️",
        "parroquia":  "⛪",
        "nota":      "ℹ️",
    }

    for key, val in data.items():
        if not val:
            continue
        key_lower = key.lower()
        emoji = next((v for k, v in claves_emoji.items() if k in key_lower), "▪️")
        lin.append(f"{emoji}  *{escape_md(key)}:*")
        lin.append(f"    `{escape_md(str(val), True)}`")

    if planilla_texto and planilla_texto.strip():
        frag = planilla_texto.strip()
        if len(frag) > SENIAT_PLANILLA_MAX_CHARS:
            frag = frag[:SENIAT_PLANILLA_MAX_CHARS].rstrip() + "\n…"
        lin.append("")
        lin.append(f"📄 *{escape_md('Planilla RIF (texto del PDF):')}*")
        lin.append(f"`{escape_md(frag, True)}`")

    return "\n".join(lin)


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
        "  • Vehículos registrados \\(INTT\\)\n"
        "  • Datos fiscales del SENIAT 🆕\n\n"
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

    # Consultar las 4 fuentes al mismo tiempo
    loop = asyncio.get_event_loop()
    result_cedula, result_ivss, result_intt, result_seniat = await asyncio.gather(
        loop.run_in_executor(None, consultar_cedula,  cedula, nacionalidad),
        loop.run_in_executor(None, consultar_ivss,    cedula, nacionalidad),
        loop.run_in_executor(None, consultar_intt,    cedula, nacionalidad),
        loop.run_in_executor(None, consultar_seniat,  cedula, nacionalidad),
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

    # ── Bloque 4: SENIAT ────────────────────────────────────────────
    try:
        if result_seniat.get("error"):
            error_seniat = result_seniat.get("error_str", "Error desconocido.")
            await update.message.reply_text(
                f"⚠️ *SENIAT:* {escape_md(error_seniat)}",
                parse_mode="MarkdownV2"
            )
        else:
            intentos = result_seniat.get("captcha_intentos", 1)
            planilla = result_seniat.get("planilla_texto")
            txt_seniat = formatear_respuesta_seniat(
                result_seniat.get("data", {}),
                nacionalidad,
                cedula,
                intentos,
                planilla_texto=planilla,
            )
            await update.message.reply_text(txt_seniat, parse_mode="MarkdownV2")
            if planilla and len(planilla.strip()) > SENIAT_PLANILLA_MAX_CHARS:
                resto = planilla.strip()[SENIAT_PLANILLA_MAX_CHARS:]
                while resto:
                    trozo = resto[:4000]
                    resto = resto[4000:]
                    await update.message.reply_text(
                        "📄 Planilla (continuación del PDF)\n\n" + trozo,
                        parse_mode=None,
                    )
    except Exception as e:
        logger.error(f"Error enviando mensaje SENIAT: {e}")
        await update.message.reply_text(
            "❌ *SENIAT:* Error al formatear la respuesta\\.",
            parse_mode="MarkdownV2"
        )

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
