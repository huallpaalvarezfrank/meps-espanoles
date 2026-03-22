"""
limpiar_sectores.py
───────────────────
Consolida sectores duplicados en la BD y aplica correcciones manuales.

Uso:
    python limpiar_sectores.py              # consolida + aplica correcciones
    python limpiar_sectores.py --dry-run    # muestra cambios sin escribir nada
    python limpiar_sectores.py --db otra.db
"""

import sqlite3
import csv
import os
import argparse
from datetime import datetime

DEFAULT_DB       = "meps_es_reuniones.db"
CORRECCIONES_CSV = "correcciones.csv"

# Sectores duplicados o mal escritos → su valor canónico
# Estos son los mismos que reconoce el portal y clasificar.py
CONSOLIDACIONES = {
    "Banca y finanzas":            "Banca",
    "Consultoría y comunicación":  "Consultoría",
    "Administración pública":      "Institución pública",
    "Telecomunicaciones":          "Tecnología",
    "Medios de comunicación":      "Medios",
    "Farmacéutica":                "Salud",
    "Agricultura y alimentación":  "Agricultura",
    "Aeronáutica y defensa":       "Defensa",
    "Discapacidad y social":       "ONG",
    "ONG y derechos humanos":      "ONG",
    "Academia e investigación":    "Educación",
    "Partidos políticos":          "Institución pública",
    "Diplomacia":                  "Institución pública",
}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def main():
    parser = argparse.ArgumentParser(description="Consolida sectores duplicados en la BD")
    parser.add_argument("--db",      default=DEFAULT_DB)
    parser.add_argument("--dry-run", action="store_true", help="Muestra cambios sin escribir")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    total_consolidados = 0
    total_correcciones = 0

    # ── 1. Consolidar duplicados ─────────────────────────────────────────────
    log("=" * 60)
    log("PASO 1 · Consolidar sectores duplicados")
    log("=" * 60)
    for viejo, nuevo in CONSOLIDACIONES.items():
        n = conn.execute(
            "SELECT COUNT(*) FROM reuniones WHERE sector=?", [viejo]
        ).fetchone()[0]
        if n > 0:
            log(f"  {viejo!r:40s} → {nuevo!r}  ({n} registros)")
            if not args.dry_run:
                conn.execute(
                    "UPDATE reuniones SET sector=? WHERE sector=?", [nuevo, viejo]
                )
            total_consolidados += n

    if total_consolidados == 0:
        log("  No hay sectores duplicados — la BD ya está limpia.")

    # ── 2. Correcciones manuales desde correcciones.csv ─────────────────────
    log("=" * 60)
    log(f"PASO 2 · Correcciones manuales ({CORRECCIONES_CSV})")
    log("=" * 60)

    if not os.path.exists(CORRECCIONES_CSV):
        log(f"  No se encontró {CORRECCIONES_CSV} — creando plantilla...")
        with open(CORRECCIONES_CSV, "w", newline="", encoding="utf-8-sig") as f:
            f.write("reunion_con;sector_correcto\n")
            f.write("# Añade filas para corregir clasificaciones incorrectas.\n")
            f.write("# reunion_con es el texto exacto del campo reunion_con en la BD.\n")
            f.write("# Ejemplo:\n")
            f.write("# Oposición venezolana;ONG\n")
            f.write("# Plataforma Unitaria Democrática;ONG\n")
        log(f"  Plantilla creada. Edítala y vuelve a ejecutar el script.")
    else:
        with open(CORRECCIONES_CSV, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=";")
            for fila in reader:
                reunion_con   = fila.get("reunion_con",    "").strip()
                sector_nuevo  = fila.get("sector_correcto","").strip()
                # Ignorar líneas de comentario o vacías
                if not reunion_con or reunion_con.startswith("#") or not sector_nuevo:
                    continue
                # Buscar coincidencias exactas en reunion_con O en el campo actores
                n = conn.execute(
                    """SELECT COUNT(*) FROM reuniones
                       WHERE (reunion_con = ? OR actores LIKE ?)
                         AND (sector != ? OR sector IS NULL)""",
                    [reunion_con, f"%{reunion_con}%", sector_nuevo]
                ).fetchone()[0]
                if n > 0:
                    log(f"  {reunion_con!r:40s} → sector {sector_nuevo!r}  ({n} registros)")
                    if not args.dry_run:
                        conn.execute(
                            """UPDATE reuniones SET sector=?
                               WHERE (reunion_con = ? OR actores LIKE ?)
                                 AND (sector != ? OR sector IS NULL)""",
                            [sector_nuevo, reunion_con, f"%{reunion_con}%", sector_nuevo]
                        )
                    total_correcciones += n

        if total_correcciones == 0:
            log("  No se aplicaron correcciones (ya estaban bien o no hubo coincidencias).")

    # ── Commit ───────────────────────────────────────────────────────────────
    if not args.dry_run:
        conn.commit()
        log("=" * 60)
        log(f"✓ {total_consolidados} consolidaciones + {total_correcciones} correcciones aplicadas.")
    else:
        log("=" * 60)
        log(f"[DRY-RUN] Se modificarían {total_consolidados} + {total_correcciones} registros.")

    # ── 3. Distribución final ─────────────────────────────────────────────────
    log("=" * 60)
    log("Distribución de sectores tras la limpieza:")
    log("=" * 60)
    rows = conn.execute(
        "SELECT sector, COUNT(*) n FROM reuniones "
        "WHERE sector IS NOT NULL AND sector!='' "
        "GROUP BY sector ORDER BY n DESC"
    ).fetchall()
    for sector, n in rows:
        print(f"  {n:>5}  {sector}")

    conn.close()


if __name__ == "__main__":
    main()
