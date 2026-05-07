"""
Consultas a la base GNB en SQLite (solo lectura).

Genera la base con: python import_gnb_sqlite.py
Ruta por defecto: BASE DE DATOS/gnb.sqlite

Variables de entorno (p. ej. Render):
  GNB_DB                 — ruta al .sqlite (ej. /tmp/gnb.sqlite)
  GNB_DOWNLOAD_URL       — descarga si falta el archivo
  GNB_DOWNLOAD_TOKEN     — opcional (GitHub privado, etc.)
  GNB_DOWNLOAD_AUTH      — opcional: bearer
"""

from __future__ import annotations

import os
import sqlite3
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_download_lock = threading.Lock()

_SQLITE_MAGIC = b"SQLite format 3"

_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_DB = _PROJECT_ROOT / "BASE DE DATOS" / "gnb.sqlite"


def _raise_bad_github_release_url() -> None:
    raise FileNotFoundError(
        "GNB_DOWNLOAD_URL no debe ser la página del release (/releases/tag/…).\n"
        "Usa el enlace directo al archivo .sqlite (/releases/download/…/gnb.sqlite).\n"
        "Ejemplo:\n"
        "https://github.com/vicentevazquez333-lang/block_333_bot/releases/download/GNB-1/gnb.sqlite"
    )


def _assert_sqlite_file(path: Path) -> None:
    """Comprueba cabecera SQLite; si no, borra el archivo y avisa (p. ej. se descargó HTML)."""
    try:
        with open(path, "rb") as f:
            hdr = f.read(len(_SQLITE_MAGIC))
    except OSError:
        return
    if hdr == _SQLITE_MAGIC:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
    raise FileNotFoundError(
        "El archivo en GNB_DB no es un SQLite válido (suele pasar si la URL descargó "
        "una página HTML en lugar del .sqlite). Corrige GNB_DOWNLOAD_URL y vuelve a intentar."
    )


def db_path() -> Path:
    override = os.environ.get("GNB_DB", "").strip()
    return Path(override) if override else _DEFAULT_DB


def _download_gnb_db(url: str, dest: Path) -> None:
    if "/releases/tag/" in url:
        _raise_bad_github_release_url()
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    if part.exists():
        part.unlink()
    try:
        headers = {"User-Agent": "gnb-bot/1"}
        token = os.environ.get("GNB_DOWNLOAD_TOKEN", "").strip()
        if token:
            if os.environ.get("GNB_DOWNLOAD_AUTH", "token").lower() == "bearer":
                headers["Authorization"] = f"Bearer {token}"
            else:
                headers["Authorization"] = f"token {token}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=1200) as resp, open(part, "wb") as out:
            while True:
                chunk = resp.read(8 * 1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        _assert_sqlite_file(part)
        part.replace(dest)
    except Exception:
        if part.exists():
            part.unlink(missing_ok=True)
        raise


def ensure_gnb_database() -> None:
    path = db_path()
    if path.is_file():
        _assert_sqlite_file(path)
        return
    url = os.environ.get("GNB_DOWNLOAD_URL", "").strip()
    if not url:
        return
    if "/releases/tag/" in url:
        _raise_bad_github_release_url()
    with _download_lock:
        if path.is_file():
            return
        try:
            _download_gnb_db(url, path)
        except urllib.error.HTTPError as e:
            raise FileNotFoundError(
                f"Error HTTP {e.code} al descargar la base GNB desde la URL configurada."
            ) from e
        except urllib.error.URLError as e:
            raise FileNotFoundError(
                f"No se pudo descargar la base GNB: {e.reason}"
            ) from e


def _connect_readonly() -> sqlite3.Connection:
    ensure_gnb_database()
    path = db_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe la base SQLite: {path}\n"
            "Opciones: python import_gnb_sqlite.py en local, "
            "o GNB_DOWNLOAD_URL + GNB_DB escribible (ej. /tmp/gnb.sqlite)."
        )
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _solo_digitos(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def buscar_por_cedula(cedula: str, *, limit: int = 5) -> list[dict[str, Any]]:
    c = _solo_digitos(cedula)
    if not c:
        return []
    lim = max(1, min(limit, 50))
    with _connect_readonly() as conn:
        cur = conn.execute(
            """SELECT cedula, siglas, codigo, apellidos_nombres, fecha_nacimiento,
                      fecha_ingreso, anos_servicio, fecha_ultimo_ascenso, anos_en_el_grado,
                      ubicacion, cargo, correo
               FROM gnb WHERE cedula = ? LIMIT ?""",
            (c, lim),
        )
        return [dict(r) for r in cur.fetchall()]


def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def buscar_por_nombre(fragmento: str, *, limit: int = 15) -> tuple[list[dict[str, Any]], bool]:
    q = (fragmento or "").strip()
    if len(q) < 3:
        return [], False
    lim = max(1, min(limit + 1, 52))
    pattern = f"%{_escape_like(q)}%"
    with _connect_readonly() as conn:
        cur = conn.execute(
            """SELECT cedula, siglas, codigo, apellidos_nombres, fecha_nacimiento,
                      fecha_ingreso, anos_servicio, fecha_ultimo_ascenso, anos_en_el_grado,
                      ubicacion, cargo, correo
               FROM gnb WHERE apellidos_nombres LIKE ? ESCAPE '\\' COLLATE NOCASE
               LIMIT ?""",
            (pattern, lim),
        )
        rows = [dict(r) for r in cur.fetchall()]
    if len(rows) > limit:
        return rows[:limit], True
    return rows, False


def compactar_fila(r: dict[str, Any], *, max_campo: int = 280) -> str:
    def cut(s: str) -> str:
        t = (s or "").strip()
        return t if len(t) <= max_campo else t[: max_campo - 1] + "…"

    partes = [
        f"🪪 {r.get('cedula', '')} — {r.get('siglas', '')} (cód. {r.get('codigo', '')})",
        f"👤 {cut(str(r.get('apellidos_nombres', '')))}",
        f"📅 Nac: {r.get('fecha_nacimiento', '')}  Ingreso: {r.get('fecha_ingreso', '')}",
        f"📊 Años serv.: {r.get('anos_servicio', '')}  Últ. ascenso: {r.get('fecha_ultimo_ascenso', '')}",
        f"📍 {cut(str(r.get('ubicacion', '')))}",
        f"💼 {cut(str(r.get('cargo', '')))}",
    ]
    co = (r.get("correo") or "").strip()
    if co:
        partes.append(f"✉️ {co}")
    return "\n".join(partes)
