"""
Importa PNB.pdf a SQLite para consultas por cédula o nombre.

Uso:
    python import_pnb_sqlite.py

Opciones:
    --pdf RUTA     Archivo PDF fuente (default: BASE DE DATOS/PNB.pdf)
    --db RUTA      Salida SQLite (default: BASE DE DATOS/pnb.sqlite)
    --limit N      Máximo de coincidencias a insertar (pruebas)
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from pathlib import Path

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parent
DEFAULT_PDF = ROOT / "BASE DE DATOS" / "PNB.pdf"
DEFAULT_DB = ROOT / "BASE DE DATOS" / "pnb.sqlite"
BATCH_SIZE = 5_000

RECORD_PATTERNS = (
    re.compile(
        r"(?P<doc>[VE]\d{6,10})\s+"
        r"(?P<nombre>[A-ZÁÉÍÓÚÑÜ�'´`.\-? ]+?)\s+"
        r"(?P<monto_nomina>\d+[.,]\d+)\s*"
        r"(?P<codigo>\d+)\s*"
        r"(?P<monto_base>\d+[.,]\d+)\s*"
        r"(?P<fecha>\d{2}/\d{2}/\d{2})\s+"
        r"(?P<estatus>SIN\s+MOVIMIENTOS)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"(?P<estatus>SIN\s+MOVIMIENTOS)\s*"
        r"(?P<monto_base>\d+[.,]\d+)\s*"
        r"(?P<fecha>\d{2}/\d{2}/\d{2})\s*"
        r"(?P<monto_nomina>\d+[.,]\d+)\s*"
        r"(?P<nombre>[A-ZÁÉÍÓÚÑÜ�'´`.\-? ]+?)\s*"
        r"(?P<codigo>\d+)\s*"
        r"(?P<doc>[VE]\d{6,10})",
        flags=re.IGNORECASE,
    ),
)


def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_name(name: str) -> str:
    name = _normalize_spaces(name).upper()
    blocked = (
        "CUERPO DE POLICIA NACIONAL BOLIVARIANA",
        "TOTAL APORTES",
        "ASEGURADOS ACTIVOS",
        "DEUDA ACUMULADA",
    )
    if any(token in name for token in blocked):
        return ""
    return name


def extract_pdf_text(pdf_path: Path) -> str:
    if PdfReader is None:
        raise RuntimeError(
            "No está disponible pypdf. Instálalo con: pip install pypdf"
        )
    reader = PdfReader(str(pdf_path))
    chunks: list[str] = []
    for page in reader.pages:
        txt = page.extract_text() or ""
        if txt:
            chunks.append(txt)
    if not chunks:
        raise RuntimeError("No se pudo extraer texto del PDF.")
    text = "\n".join(chunks)
    text = re.sub(r"--\s*\d+\s+of\s+\d+\s*--", " ", text, flags=re.IGNORECASE)
    return text


def parse_records(text: str, limit: int | None = None) -> list[tuple[str, ...]]:
    normalized = _normalize_spaces(text.upper())
    found: list[tuple[str, ...]] = []
    seen: set[tuple[str, str, str]] = set()

    for pattern in RECORD_PATTERNS:
        for m in pattern.finditer(normalized):
            doc = m.group("doc").strip().upper()
            nacionalidad = doc[0]
            cedula = "".join(c for c in doc[1:] if c.isdigit())
            if not cedula:
                continue
            nombre = _clean_name(m.group("nombre"))
            if len(nombre) < 6:
                continue
            codigo = _normalize_spaces(m.group("codigo"))
            monto_nomina = _normalize_spaces(m.group("monto_nomina"))
            monto_base = _normalize_spaces(m.group("monto_base"))
            fecha = _normalize_spaces(m.group("fecha"))
            estatus = _normalize_spaces(m.group("estatus")).upper()
            row = (
                cedula,
                nacionalidad,
                nombre,
                codigo,
                monto_nomina,
                monto_base,
                fecha,
                estatus,
            )
            uniq = (cedula, nombre, fecha)
            if uniq in seen:
                continue
            seen.add(uniq)
            found.append(row)
            if limit is not None and len(found) >= limit:
                return found
    return found


def main() -> int:
    ap = argparse.ArgumentParser(description="Importar PNB.pdf a SQLite indexado.")
    ap.add_argument("--pdf", type=Path, default=DEFAULT_PDF, help="Archivo PDF fuente")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="SQLite de salida")
    ap.add_argument("--limit", type=int, default=None, help="Máximo de filas a insertar")
    args = ap.parse_args()

    pdf_path: Path = args.pdf
    db_path: Path = args.db

    if not pdf_path.is_file():
        print(f"ERROR: no existe el archivo: {pdf_path}", file=sys.stderr)
        return 1

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    print(f"Fuente: {pdf_path}")
    print(f"Salida: {db_path}")
    t0 = time.perf_counter()

    text = extract_pdf_text(pdf_path)
    rows = parse_records(text, limit=args.limit)
    if not rows:
        print("ERROR: no se detectaron filas válidas en el PDF.", file=sys.stderr)
        return 2

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        conn.execute(
            """CREATE TABLE pnb (
                cedula       TEXT NOT NULL,
                nacionalidad TEXT NOT NULL,
                nombre       TEXT NOT NULL,
                codigo       TEXT,
                monto_nomina TEXT,
                monto_base   TEXT,
                fecha        TEXT,
                estatus      TEXT
            )"""
        )

        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            conn.executemany(
                "INSERT INTO pnb VALUES (?,?,?,?,?,?,?,?)",
                batch,
            )
            conn.commit()

        print(f"Insertadas: {len(rows):,} filas")
        print("Creando índices…")
        conn.execute("CREATE INDEX idx_pnb_cedula ON pnb(cedula)")
        conn.execute("CREATE INDEX idx_pnb_nombre ON pnb(nombre COLLATE NOCASE)")
        conn.commit()

        conn.execute("PRAGMA analysis_limit = 400")
        conn.execute("ANALYZE pnb")
        conn.commit()

        elapsed = time.perf_counter() - t0
        size_mib = db_path.stat().st_size / (1024**2)
        print(f"Listo en {elapsed:.1f} s. Tamaño DB: {size_mib:.2f} MiB")
    finally:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = FULL")
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
