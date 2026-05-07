"""
Consultas a la base Digitel en SQLite (solo lectura).

Genera la base con: python import_digitel_sqlite.py
Ruta por defecto: BASE DE DATOS/digitel.sqlite
Override: variable de entorno DIGITEL_DB (ruta absoluta al .sqlite)

Si DIGITEL_DOWNLOAD_URL está definida y el archivo aún no existe, se descarga
una vez (útil en Render sin disco persistente). Opcional: DIGITEL_DOWNLOAD_TOKEN
para URLs que requieren auth (p. ej. asset de Release en repo privado de GitHub).
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


def _raise_bad_github_release_url_digitel() -> None:
    raise FileNotFoundError(
        "DIGITEL_DOWNLOAD_URL no debe ser /releases/tag/… (página HTML).\n"
        "Usa: …/releases/download/TAG_ARCHIVO/digitel.sqlite"
    )


def _assert_sqlite_file_digitel(path: Path) -> None:
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
        "El archivo en DIGITEL_DB no es SQLite válido (URL incorrecta o HTML descargado). "
        "Corrige DIGITEL_DOWNLOAD_URL."
    )


# Raíz del proyecto (directorio donde está este archivo)
_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_DB = _PROJECT_ROOT / "BASE DE DATOS" / "digitel.sqlite"


def db_path() -> Path:
    override = os.environ.get("DIGITEL_DB", "").strip()
    return Path(override) if override else _DEFAULT_DB


def _download_digitel_db(url: str, dest: Path) -> None:
    if "/releases/tag/" in url:
        _raise_bad_github_release_url_digitel()
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    if part.exists():
        part.unlink()
    try:
        headers = {"User-Agent": "digitel-bot/1"}
        token = os.environ.get("DIGITEL_DOWNLOAD_TOKEN", "").strip()
        if token:
            # GitHub Releases (repo privado): Authorization: token <PAT>
            if os.environ.get("DIGITEL_DOWNLOAD_AUTH", "token").lower() == "bearer":
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
        _assert_sqlite_file_digitel(part)
        part.replace(dest)
    except Exception:
        if part.exists():
            part.unlink(missing_ok=True)
        raise


def ensure_digitel_database() -> None:
    """
    Si falta el .sqlite y existe DIGITEL_DOWNLOAD_URL, descarga a la ruta de DIGITEL_DB.
    En Render suele usarse DIGITEL_DB=/tmp/digitel.sqlite.
    """
    path = db_path()
    if path.is_file():
        _assert_sqlite_file_digitel(path)
        return
    url = os.environ.get("DIGITEL_DOWNLOAD_URL", "").strip()
    if not url:
        return
    if "/releases/tag/" in url:
        _raise_bad_github_release_url_digitel()
    with _download_lock:
        if path.is_file():
            return
        try:
            _download_digitel_db(url, path)
        except urllib.error.HTTPError as e:
            raise FileNotFoundError(
                f"Error HTTP {e.code} al descargar la base Digitel desde la URL configurada."
            ) from e
        except urllib.error.URLError as e:
            raise FileNotFoundError(
                f"No se pudo descargar la base Digitel: {e.reason}"
            ) from e


def _connect_readonly() -> sqlite3.Connection:
    ensure_digitel_database()
    path = db_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"No existe la base SQLite: {path}\n"
            "Opciones: python import_digitel_sqlite.py en local, "
            "o DIGITEL_DOWNLOAD_URL + DIGITEL_DB escribible (ej. /tmp/digitel.sqlite)."
        )
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _solo_digitos(s: str) -> str:
    return "".join(c for c in (s or "") if c.isdigit())


def _buscar_columna(
    columna: str,
    valor: str,
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    if columna not in ("telefono", "documento"):
        raise ValueError("columna inválida")
    lim = max(1, min(limit + 1, 502))
    with _connect_readonly() as conn:
        cur = conn.execute(
            f"SELECT tipo, documento, telefono FROM digitel WHERE {columna} = ? LIMIT ?",
            (valor, lim),
        )
        rows = [dict(r) for r in cur.fetchall()]
    if len(rows) > limit:
        return rows[:limit], True
    return rows, False


def buscar_por_telefono(telefono: str, *, limit: int = 100) -> tuple[list[dict[str, Any]], bool]:
    """Coincidencia exacta en telefono (entrada normalizada a solo dígitos)."""
    tel = _solo_digitos(telefono)
    if not tel:
        return [], False
    return _buscar_columna("telefono", tel, limit=limit)


def buscar_por_documento(documento: str, *, limit: int = 100) -> tuple[list[dict[str, Any]], bool]:
    """Coincidencia exacta en documento (solo dígitos, como en el TXT)."""
    doc = _solo_digitos(documento)
    if not doc:
        return [], False
    return _buscar_columna("documento", doc, limit=limit)
