"""
Registro local de mensajes de texto en chats con el bot y exportación a PDF.

Importante: Telegram no entrega el historial previo; solo se guarda lo que el bot
recibe mientras está en ejecución y el usuario está autorizado (misma lista blanca).

Variables de entorno:
  CHAT_LOG_DB              — ruta al SQLite (default: BASE DE DATOS/chat_log.sqlite)
  CHAT_EXPORT_MAX_LINES    — máximo de líneas guardadas por chat (default 8000)
"""

from __future__ import annotations

import io
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from fpdf import FPDF
except ImportError:  # pragma: no cover
    FPDF = None  # type: ignore[misc, assignment]

_LOCK = threading.Lock()

_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_DB = _PROJECT_ROOT / "BASE DE DATOS" / "chat_log.sqlite"


def db_path() -> Path:
    override = os.environ.get("CHAT_LOG_DB", "").strip()
    return Path(override) if override else _DEFAULT_DB


def _max_lines_per_chat() -> int:
    raw = os.environ.get("CHAT_EXPORT_MAX_LINES", "8000").strip()
    try:
        n = int(raw)
        return max(1, min(n, 100_000))
    except ValueError:
        return 8000


def _connect() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS chat_lines (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    display_name TEXT,
                    created_at TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    body TEXT NOT NULL
                )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chat_lines_chat ON chat_lines(chat_id)"
            )
            conn.commit()
        finally:
            conn.close()


def _trim_chat(conn: sqlite3.Connection, chat_id: int) -> None:
    lim = _max_lines_per_chat()
    row = conn.execute(
        "SELECT COUNT(*) FROM chat_lines WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    n = int(row[0]) if row else 0
    if n <= lim:
        return
    excess = n - lim
    old_ids = [
        r[0]
        for r in conn.execute(
            "SELECT id FROM chat_lines WHERE chat_id = ? ORDER BY id ASC LIMIT ?",
            (chat_id, excess),
        ).fetchall()
    ]
    if not old_ids:
        return
    q = "DELETE FROM chat_lines WHERE id IN (%s)" % ",".join("?" * len(old_ids))
    conn.execute(q, old_ids)


def append_line(
    *,
    chat_id: int,
    user_id: int,
    username: str | None,
    display_name: str | None,
    body: str,
    kind: str = "msg",
) -> None:
    text = (body or "").strip()
    if not text:
        return
    if len(text) > 12000:
        text = text[:11999] + "…"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    un = (username or "").strip() or None
    dn = (display_name or "").strip() or None
    with _LOCK:
        conn = _connect()
        try:
            conn.execute(
                """INSERT INTO chat_lines
                   (chat_id, user_id, username, display_name, created_at, kind, body)
                   VALUES (?,?,?,?,?,?,?)""",
                (chat_id, user_id, un, dn, now, kind, text),
            )
            _trim_chat(conn, chat_id)
            conn.commit()
        finally:
            conn.close()


def fetch_lines(chat_id: int) -> list[dict[str, Any]]:
    with _LOCK:
        conn = _connect()
        try:
            cur = conn.execute(
                """SELECT id, user_id, username, display_name, created_at, kind, body
                   FROM chat_lines WHERE chat_id = ? ORDER BY id ASC""",
                (chat_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()


def clear_chat(chat_id: int) -> int:
    with _LOCK:
        conn = _connect()
        try:
            cur = conn.execute("DELETE FROM chat_lines WHERE chat_id = ?", (chat_id,))
            conn.commit()
            return cur.rowcount or 0
        finally:
            conn.close()


def _pdf_safe_line(s: str) -> str:
    """Helvetica WinAnsi: preservar español típico vía Latin-1."""
    return (s or "").encode("latin-1", errors="replace").decode("latin-1")


def build_pdf(chat_id: int, *, chat_title: str | None = None) -> tuple[bytes, str]:
    """
    Genera PDF en memoria. Devuelve (bytes, nombre_archivo sugerido).
    """
    rows = fetch_lines(chat_id)
    if not rows:
        raise ValueError("NO_ROWS")

    if FPDF is None:
        raise RuntimeError("fpdf2 no está instalado")

    title = chat_title or f"chat_{chat_id}"
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in title)[:80]
    fname = f"historial_{safe_name}_{int(time.time())}.pdf"

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, _pdf_safe_line(f"Historial del chat {chat_id}"), ln=True)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(
        0,
        6,
        _pdf_safe_line(
            f"Generado en servidor (UTC). Lineas exportadas: {len(rows)}. "
            "Solo incluye mensajes de texto registrados mientras el bot estaba activo."
        ),
        ln=True,
    )
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    for r in rows:
        who_parts: list[str] = []
        uid = r.get("user_id")
        if uid is not None:
            who_parts.append(str(uid))
        if r.get("username"):
            who_parts.append("@" + str(r["username"]))
        if r.get("display_name"):
            who_parts.append(str(r["display_name"]))
        head = " · ".join(who_parts) if who_parts else "?"
        line = f"[{r.get('created_at','')}] ({head})"
        body = str(r.get("body", ""))
        block = _pdf_safe_line(line) + "\n" + _pdf_safe_line(body)
        pdf.multi_cell(0, 5, block)
        pdf.ln(2)

    buf = io.BytesIO()
    pdf.output(buf)
    buf.seek(0)
    out = buf.getvalue()
    return out, fname


def build_txt(chat_id: int, *, chat_title: str | None = None) -> tuple[bytes, str]:
    rows = fetch_lines(chat_id)
    if not rows:
        raise ValueError("NO_ROWS")
    title = chat_title or f"chat_{chat_id}"
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in title)[:80]
    fname = f"historial_{safe_name}_{int(time.time())}.txt"
    lines_out: list[str] = []
    for r in rows:
        who = f"{r.get('user_id')} @{r.get('username') or ''} {r.get('display_name') or ''}".strip()
        lines_out.append(f"[{r.get('created_at','')}] ({who})\n{r.get('body','')}\n")
    data = "\n".join(lines_out).encode("utf-8")
    return data, fname
