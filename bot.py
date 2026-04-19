"""
Bot de Telegram - Consulta de Cédulas Venezolanas
──────────────────────────────────────────────────
• En LOCAL:      usa polling (sin configuración extra)
• En PRODUCCIÓN: usa webhooks (Render / Koyeb / Railway)

Instalar dependencias:
    pip install -r requirements.txt
"""

import os
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
    # El dropdown del IVSS espera 'Venezolano' o 'Extranjero'
    nac_map = {"V": "Venezolano", "E": "Extranjero"}
    payload = {
        "nacionalidad": nac_map.get(nacionalidad, "Venezolano"),
        "cedula":       cedula,
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

        # Extraer todos los pares etiqueta→valor de la tabla de resultados
        data = {}
        # Buscar celdas cabecera (th) y de dato (td) en pares
        rows = soup.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            # Filas con exactamente 2 celdas: clave - valor
            if len(cells) == 2:
                key   = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                if key:
                    data[key] = value

        if not data:
            return {"error": True, "error_str": "⚠️ El IVSS no devolvió datos para esta cédula."}

        return {"error": False, "data": data}

    except requests.exceptions.Timeout:
        return {"error": True, "error_str": "⏱️ El IVSS tardó demasiado. Intenta de nuevo."}
    except requests.exceptions.ConnectionError:
        return {"error": True, "error_str": "🔌 No se pudo conectar al servidor del IVSS."}
    except Exception as e:
        return {"error": True, "error_str": f"Error inesperado en IVSS: {str(e)}"}


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
        f"🧾  *R\.I\.F\.:*         `{rif}`\n"
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
        """Escapa caracteres especiales de MarkdownV2."""
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
        "nombre":              "👤",
        "nombres":             "👤",
        "apellidos":           "👤",
        "semanas":             "📊",
        "cotizadas":           "📊",
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
        "🔍 Consultando *dos fuentes* en paralelo\\.\\.\\. un momento ⏳",
        parse_mode="MarkdownV2",
    )

    # Consultar ambas APIs al mismo tiempo en hilos separados (son bloqueantes)
    loop = asyncio.get_event_loop()
    result_cedula, result_ivss = await asyncio.gather(
        loop.run_in_executor(None, consultar_cedula, cedula, nacionalidad),
        loop.run_in_executor(None, consultar_ivss,   cedula, nacionalidad),
    )

    # ── Bloque 1: Datos de cédula.com.ve ───────────────────────────────
    if result_cedula.get("error"):
        error = result_cedula.get("error_str", "Error desconocido.")
        await msg.edit_text(
            f"❌ *Error al consultar cédula:*\n`{error}`",
            parse_mode="MarkdownV2",
        )
        return

    data_cedula = result_cedula.get("data", {})
    if not data_cedula:
        await msg.edit_text(
            "❌ Cédula no encontrada en la base de datos\\.",
            parse_mode="MarkdownV2",
        )
        return

    keyboard = [[InlineKeyboardButton("🔄 Nueva consulta", callback_data="NUEVA_CONSULTA")]]
    await msg.edit_text(
        formatear_respuesta(data_cedula),
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    # ── Bloque 2: Datos del IVSS ────────────────────────────────────────
    if result_ivss.get("error"):
        error_ivss = result_ivss.get("error_str", "Error desconocido en IVSS.")
        await update.message.reply_text(
            f"⚠️ *IVSS:* {error_ivss}",
            parse_mode="MarkdownV2",
        )
    else:
        data_ivss = result_ivss.get("data", {})
        await update.message.reply_text(
            formatear_respuesta_ivss(data_ivss, nacionalidad, cedula),
            parse_mode="MarkdownV2",
        )

    logger.info("Consultada: %s-%s", nacionalidad, cedula)


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
