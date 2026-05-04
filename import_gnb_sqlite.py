"""
Importa BASE DE DATOS/GNB.txt (TSV con cabecera) a SQLite.

Columnas: cédula, siglas, código, apellidos y nombres, fechas, años de servicio,
ubicación, cargo, correo.

Uso:
    python import_gnb_sqlite.py

Prueba:
    python import_gnb_sqlite.py --limit 1000

Opciones:
    --txt RUTA     Fuente (default: BASE DE DATOS/GNB.txt)
    --db RUTA      Salida (default: BASE DE DATOS/gnb.sqlite)
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEFAULT_TXT = ROOT / "BASE DE DATOS" / "GNB.txt"
DEFAULT_DB = ROOT / "BASE DE DATOS" / "gnb.sqlite"
BATCH_SIZE = 5_000

COLS = (
    "cedula",
    "siglas",
    "codigo",
    "apellidos_nombres",
    "fecha_nacimiento",
    "fecha_ingreso",
    "anos_servicio",
    "fecha_ultimo_ascenso",
    "anos_en_el_grado",
    "ubicacion",
    "cargo",
    "correo",
)


def normalize_cells(cells: list[str]) -> tuple[str, ...]:
    """12 columnas; si sobran tabs en texto largo, se unen al final (correo/cargo)."""
    if len(cells) < 12:
        cells = cells + [""] * (12 - len(cells))
    elif len(cells) > 12:
        head = cells[:11]
        head.append("\t".join(cells[11:]))
        cells = head
    return tuple(c.strip() for c in cells[:12])


def main() -> int:
    ap = argparse.ArgumentParser(description="Importar GNB.txt a SQLite indexado.")
    ap.add_argument("--txt", type=Path, default=DEFAULT_TXT, help="Archivo TSV fuente")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB, help="Ruta del SQLite de salida")
    ap.add_argument("--limit", type=int, default=None, help="Máximo de filas de datos (pruebas)")
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

    placeholders = ",".join(["?"] * 12)
    insert_sql = f"INSERT INTO gnb VALUES ({placeholders})"

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = OFF")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA locking_mode = EXCLUSIVE")

        col_defs = ", ".join(f'{c} TEXT' for c in COLS)
        conn.execute(f"CREATE TABLE gnb ({col_defs})")

        batch: list[tuple[str, ...]] = []
        committed = 0
        skipped = 0
        t0 = time.perf_counter()

        with open(txt_path, encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if args.limit is not None and committed + len(batch) >= args.limit:
                    break
                if not row or all(not (c or "").strip() for c in row):
                    skipped += 1
                    continue
                first = (row[0] or "").strip().upper().lstrip("\ufeff")
                if first == "CEDULA" or first.startswith("CEDULA"):
                    continue
                tup = normalize_cells([c or "" for c in row])
                if not tup[0] or not tup[0].isdigit():
                    skipped += 1
                    continue
                batch.append(tup)
                if len(batch) >= BATCH_SIZE:
                    conn.executemany(insert_sql, batch)
                    conn.commit()
                    committed += len(batch)
                    batch.clear()
                    print(f"  … {committed:,} filas", flush=True)

        if batch:
            conn.executemany(insert_sql, batch)
            conn.commit()
            committed += len(batch)

        print(f"Insertadas: {committed:,} filas (omitidas / cabecera: {skipped:,})")

        print("Creando índices…")
        conn.execute("CREATE INDEX idx_gnb_cedula ON gnb(cedula)")
        conn.execute(
            "CREATE INDEX idx_gnb_apellidos ON gnb(apellidos_nombres COLLATE NOCASE)"
        )
        conn.commit()

        conn.execute("PRAGMA analysis_limit = 400")
        conn.execute("ANALYZE gnb")
        conn.commit()

        elapsed = time.perf_counter() - t0
        print(f"Listo en {elapsed:.1f} s. Tamaño DB: {db_path.stat().st_size / (1024**2):.2f} MiB")
    finally:
        conn.execute("PRAGMA journal_mode = DELETE")
        conn.execute("PRAGMA synchronous = FULL")
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
