"""
Consultas a la base CICPC en SQLite (solo lectura).

Genera la base con: python import_cicpc_sqlite.py
Ruta por defecto: BASE DE DATOS/cicpc.sqlite

Variables de entorno:
  CICPC_DB             — ruta al .sqlite
  CICPC_DOWNLOAD_URL   — descarga si falta el archivo
  CICPC_DOWNLOAD_TOKEN — opcional
  CICPC_DOWNLOAD_AUTH  — opcional: bearer
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
_DEFAULT_DB = _PROJECT_ROOT / "BASE DE DATOS" / "cicpc.sqlite"


def _raise_bad_github_release_url() -> None:
    raise FileNotFoundError(
        "CICPC_DOWNLOAD_URL no debe ser la página del release (/releases/tag/…).\n"
        "Usa el enlace directo al archivo .sqlite (/releases/download/…/cicpc.sqlite)."
    )


def _assert_sqlite_file(path: Path) -> None:
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
        "El archivo configurado en CICPC_DB no es un SQLite válido."
    )


def db_path() -> Path:
    override = os.environ.get("CICPC_DB", "").strip()
    return Path(override) if override else _DEFAULT_DB


def _download_cicpc_db(url: str, dest: Path) -> None:
    if "/releases/tag/" in url:
        _raise_bad_github_release_url()
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    if part.exists():
        part.unlink()
    try:
        headers = {"User-Agent": "cicpc-bot/1"}
        token = os.environ.get("CICPC_DOWNLOAD_TOKEN", "").strip()
        if token:
            if os.environ.get("CICPC_DOWNLOAD_AUTH", "token").lower() == "bearer":
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


def ensure_cicpc_database() -> None:
    path = db_path()
    if path.is_file():
        _assert_sqlite_file(path)
        return
    url = os.environ.get("CICPC_DOWNLOAD_URL", "").strip()
    if not url:
        return
    if "/releases/tag/" in url:
        _raise_bad_github_release_url()
    with _download_lock:
        if path.is_file():
            return
        try:
            _download_cicpc_db(url, path)
        except urllib.error.HTTPError as e:
            raise FileNotFoundError(
                f"Error HTTP {e.code} al descargar la base CICPC."
            ) from e
        except urllib.error.URLError as e:
            raise FileNotFoundError(
                f"No se pudo descargar la base CICPC: {e.reason}"
            ) from e


def _connect_readonly() -> sqlite3.Connection:
    ensure_cicpc_database()
    path = db_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe la base SQLite: {path}\n"
            "Opciones: python import_cicpc_sqlite.py en local, "
            "o CICPC_DOWNLOAD_URL + CICPC_DB en la nube."
        )
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _solo_digitos(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def buscar_por_cedula(cedula: str, *, limit: int = 10) -> list[dict[str, Any]]:
    c = _solo_digitos(cedula)
    if not c:
        return []
    c_no_zero = c.lstrip("0") or "0"
    lim = max(1, min(limit, 100))
    with _connect_readonly() as conn:
        cur = conn.execute(
            """SELECT cedula, nacionalidad, nombre, codigo, monto_nomina,
                      monto_base, fecha, estatus
               FROM cicpc
               WHERE cedula = ?
                  OR ltrim(cedula, '0') = ?
               LIMIT ?""",
            (c, c_no_zero, lim),
        )
        return [dict(r) for r in cur.fetchall()]


def buscar_por_documento(documento: str, *, limit: int = 10) -> list[dict[str, Any]]:
    """
    Busca por documento con o sin nacionalidad.
    Ejemplos válidos: 17965814, V17965814, E12345678.
    """
    raw = (documento or "").strip().upper()
    if not raw:
        return []

    nacionalidad = ""
    if raw[:1] in ("V", "E"):
        nacionalidad = raw[:1]
        raw = raw[1:]

    c = _solo_digitos(raw)
    if not c:
        return []
    c_no_zero = c.lstrip("0") or "0"
    lim = max(1, min(limit, 100))

    with _connect_readonly() as conn:
        if nacionalidad:
            cur = conn.execute(
                """SELECT cedula, nacionalidad, nombre, codigo, monto_nomina,
                          monto_base, fecha, estatus
                   FROM cicpc
                   WHERE nacionalidad = ?
                     AND (cedula = ? OR ltrim(cedula, '0') = ?)
                   LIMIT ?""",
                (nacionalidad, c, c_no_zero, lim),
            )
        else:
            cur = conn.execute(
                """SELECT cedula, nacionalidad, nombre, codigo, monto_nomina,
                          monto_base, fecha, estatus
                   FROM cicpc
                   WHERE cedula = ?
                      OR ltrim(cedula, '0') = ?
                   LIMIT ?""",
                (c, c_no_zero, lim),
            )
        return [dict(r) for r in cur.fetchall()]


def buscar_por_nombre(fragmento: str, *, limit: int = 15) -> tuple[list[dict[str, Any]], bool]:
    q = (fragmento or "").strip()
    if len(q) < 3:
        return [], False
    lim = max(1, min(limit + 1, 101))
    pattern = f"%{_escape_like(q)}%"
    with _connect_readonly() as conn:
        cur = conn.execute(
            """SELECT cedula, nacionalidad, nombre, codigo, monto_nomina,
                      monto_base, fecha, estatus
               FROM cicpc
               WHERE nombre LIKE ? ESCAPE '\\' COLLATE NOCASE
               LIMIT ?""",
            (pattern, lim),
        )
        rows = [dict(r) for r in cur.fetchall()]
    if len(rows) > limit:
        return rows[:limit], True
    return rows, False


def compactar_fila(r: dict[str, Any]) -> str:
    return (
        f"🪪 {r.get('nacionalidad', '')}-{r.get('cedula', '')}\n"
        f"👤 {r.get('nombre', '')}\n"
        f"📌 Código: {r.get('codigo', '')} · Fecha: {r.get('fecha', '')}\n"
        f"💵 Nómina: {r.get('monto_nomina', '')} · Base: {r.get('monto_base', '')}\n"
        f"📄 {r.get('estatus', '')}"
    )
