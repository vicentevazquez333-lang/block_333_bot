"""
Importa BASE DE DATOS/DIGITEL.TXT a SQLite con índices en telefono y documento.

Uso típico (tarda según disco; millones de filas):
    python import_digitel_sqlite.py

Prueba rápida (solo N líneas):
    python import_digitel_sqlite.py --limit 5000

Opciones:
    --txt RUTA     Archivo fuente (default: BASE DE DATOS/DIGITEL.TXT)
    --db RUTA      Salida .sqlite (default: BASE DE DATOS/digitel.sqlite)
    --limit N      Importar solo las primeras N líneas no vacías
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_TXT = ROOT / "BASE DE DATOS" / "DIGITEL.TXT"
DEFAULT_DB = ROOT / "BASE DE DATOS" / "digitel.sqlite"
BATCH_SIZE = 50_000


def parse_line(line: str) -> tuple[str, str, str] | None:
    line = line.strip("\r\n")
    if not line:
        return None
    parts = line.split("\t")
    if len(parts) < 3:
        parts = line.split()
    if len(parts) < 3:
        return None
    tipo, doc, tel = parts[0].strip(), parts[1].strip(), parts[2].strip()
    if not tipo or not doc or not tel:
        return None
    return tipo, doc, tel


def main() -> int:
    ap = argparse.ArgumentParser(description="Importar DIGITEL.TXT a SQLite indexado.")
    ap.add_argument("--txt", type=Path, default=DEFAULT_TXT, help="Archivo TSV fuente")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Ruta del SQLite de salida")
    ap.add_argument("--limit", type=int, default=None, help="Máximo de filas a importar (pruebas)")
    args = ap.parse_args()

    txt_path: Path = args.txt
    db_path: Path = args.db

    if not txt_path.is_file():
        print(f"ERROR: no existe el archivo: {txt_path}", file=sys.stderr)
        return 1

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    print(f"Fuente: {txt_path}")
    print(f"Salida: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        conn.execute(
            """CREATE TABLE digitel (
                tipo        TEXT NOT NULL,
                documento TEXT NOT NULL,
                telefono  TEXT NOT NULL
            )"""
        )

        batch: list[tuple[str, str, str]] = []
        committed = 0
        skipped = 0
        t0 = time.perf_counter()

        with open(txt_path, encoding="utf-8", errors="replace", newline="") as f:
            for line in f:
                if args.limit is not None and committed + len(batch) >= args.limit:
                    break
                parsed = parse_line(line)
                if parsed is None:
                    skipped += 1
                    continue
                batch.append(parsed)
                if len(batch) >= BATCH_SIZE:
                    conn.executemany("INSERT INTO digitel VALUES (?,?,?)", batch)
                    conn.commit()
                    committed += len(batch)
                    batch.clear()
                    print(f"  … {committed:,} filas", flush=True)

        if batch:
            conn.executemany("INSERT INTO digitel VALUES (?,?,?)", batch)
            conn.commit()
            committed += len(batch)

        print(f"Insertadas: {committed:,} filas (omitidas mal formato: {skipped:,})")

        print("Creando índices…")
        conn.execute("CREATE INDEX idx_digitel_telefono ON digitel(telefono)")
        conn.execute("CREATE INDEX idx_digitel_documento ON digitel(documento)")
        conn.commit()

        conn.execute("PRAGMA analysis_limit = 400")
        conn.execute("ANALYZE digitel")
        conn.commit()

        elapsed = time.perf_counter() - t0
        print(f"Listo en {elapsed:.1f} s. Tamaño archivo DB: {db_path.stat().st_size / (1024**2):.1f} MiB")
    finally:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = FULL")
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
