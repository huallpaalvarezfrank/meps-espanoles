"""
normalizar.py
─────────────
Normaliza la columna reunion_con de la BD de reuniones de eurodiputados.

Uso:
    python normalizar.py                          # usa rutas por defecto
    python normalizar.py --db mi_bd.db            # BD personalizada
    python normalizar.py --reglas mis_reglas.csv  # diccionario personalizado
    python normalizar.py --solo-revisar           # solo genera revisar.csv, no modifica la BD

Flujo:
    1. Lee reglas.csv  →  aplica equivalencias conocidas a la BD
    2. Detecta valores nuevos sin regla
    3. Aplica fuzzy matching contra los canónicos conocidos
    4. Genera revisar.csv con sugerencias para tu revisión
    5. Si hay decisiones tuyas en revisar.csv, las incorpora a reglas.csv
"""

import sqlite3
import csv
import os
import re
import difflib
import argparse
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN POR DEFECTO
# ─────────────────────────────────────────────────────────────────────────────
_HERE           = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB      = os.path.join(_HERE, "..", "meps_es_reuniones.db")
DEFAULT_REGLAS  = os.path.join(_HERE, "reglas.csv")
DEFAULT_REVISAR = os.path.join(_HERE, "revisar.csv")

UMBRAL_FUZZY    = 80   # similitud mínima (0-100) para mostrar sugerencia
SEPARADOR_CSV   = ";"  # separador usado en los CSV

# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def leer_reglas(path_reglas):
    """
    Lee reglas.csv y devuelve un diccionario:
        { valor_original_en_minusculas: (nombre_canonico, sector) }

    Usar minúsculas como clave permite comparación case-insensitive.
    """
    reglas = {}
    if not os.path.exists(path_reglas):
        log(f"⚠️  No se encontró {path_reglas} — se creará vacío")
        return reglas

    with open(path_reglas, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=SEPARADOR_CSV)
        for fila in reader:
            clave = fila["valor_original"].strip().lower()
            canonico = fila["nombre_canonico"].strip()
            sector   = fila.get("sector", "").strip()
            if clave and canonico:
                reglas[clave] = (canonico, sector)

    log(f"📖 {len(reglas)} reglas cargadas desde {path_reglas}")
    return reglas


def guardar_reglas(path_reglas, reglas_dict):
    """
    Guarda el diccionario de reglas de vuelta al CSV.
    reglas_dict: { valor_original_lower: (canonico, sector) }
    """
    with open(path_reglas, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=SEPARADOR_CSV)
        writer.writerow(["valor_original", "nombre_canonico", "sector"])
        for clave, (canonico, sector) in sorted(reglas_dict.items()):
            # Guardar con el texto original (la clave está en minúsculas,
            # pero el valor canónico ya tiene el formato correcto)
            writer.writerow([clave, canonico, sector])
    log(f"💾 {len(reglas_dict)} reglas guardadas en {path_reglas}")


def limpiar_texto(texto):
    """
    Limpieza básica de formato sin cambiar el contenido semántico.
    Esto se aplica siempre, incluso sin reglas explícitas.
    """
    if not texto:
        return texto
    # Colapsar espacios múltiples
    texto = re.sub(r"  +", " ", texto)
    # Quitar espacios al inicio y final
    texto = texto.strip()
    # Quitar punto final suelto
    texto = re.sub(r"\.$", "", texto).strip()
    return texto


def fuzzy_score(a, b):
    """
    Calcula similitud entre dos strings (0-100).
    Usa SequenceMatcher de difflib, que viene incluido en Python.
    """
    return int(difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100)


def buscar_mejor_sugerencia(valor, canonicos_conocidos):
    """
    Dado un valor sin regla, busca el canónico más parecido.
    Devuelve (mejor_canonico, score) o (None, 0) si no supera el umbral.
    """
    mejor_score = 0
    mejor_canonico = None

    for canonico in canonicos_conocidos:
        score = fuzzy_score(valor, canonico)
        if score > mejor_score:
            mejor_score = score
            mejor_canonico = canonico

    if mejor_score >= UMBRAL_FUZZY:
        return mejor_canonico, mejor_score
    return None, 0


# ─────────────────────────────────────────────────────────────────────────────
# PASO 1: APLICAR REGLAS A LA BD
# ─────────────────────────────────────────────────────────────────────────────

def aplicar_reglas(conn, reglas):
    """
    Para cada fila de la BD:
    - Guarda el valor original en reunion_con_original (si no estaba ya)
    - Aplica limpieza básica
    - Si hay regla explícita, aplica el nombre canónico
    - Si hay regla de sector, rellena la columna sector

    Devuelve el número de filas modificadas.
    """
    cur = conn.cursor()

    # Asegurarse de que existen las columnas auxiliares
    columnas = [r[1] for r in cur.execute("PRAGMA table_info(reuniones)").fetchall()]
    if "reunion_con_original" not in columnas:
        cur.execute("ALTER TABLE reuniones ADD COLUMN reunion_con_original TEXT")
        log("  → Columna reunion_con_original creada")
    if "sector" not in columnas:
        cur.execute("ALTER TABLE reuniones ADD COLUMN sector TEXT")
        log("  → Columna sector creada")
    if "normalizado" not in columnas:
        cur.execute("ALTER TABLE reuniones ADD COLUMN normalizado INTEGER DEFAULT 0")
        log("  → Columna normalizado creada")

    # Traer todas las filas con reunion_con
    filas = cur.execute(
        "SELECT id, reunion_con, reunion_con_original FROM reuniones WHERE reunion_con IS NOT NULL"
    ).fetchall()

    modificadas = 0

    for id_, valor, valor_orig in filas:
        if not valor:
            continue

        # Guardar original la primera vez
        if not valor_orig:
            cur.execute(
                "UPDATE reuniones SET reunion_con_original = ? WHERE id = ?",
                (valor, id_)
            )

        # Limpieza básica de formato
        valor_limpio = limpiar_texto(valor)

        # Buscar regla (case-insensitive)
        clave = valor_limpio.lower()
        if clave in reglas:
            canonico, sector = reglas[clave]
            if sector:
                cur.execute(
                    "UPDATE OR IGNORE reuniones SET reunion_con = ?, sector = ?, normalizado = 1 WHERE id = ?",
                    (canonico, sector, id_)
                )
            else:
                # Regla sin sector: actualizar nombre canónico pero NO tocar sector existente
                cur.execute(
                    "UPDATE OR IGNORE reuniones SET reunion_con = ?, normalizado = 1 WHERE id = ?",
                    (canonico, id_)
                )
            if canonico != valor or sector:
                modificadas += 1
        elif valor_limpio != valor:
            # Solo limpieza de formato, sin regla explícita
            cur.execute(
                "UPDATE OR IGNORE reuniones SET reunion_con = ?, normalizado = 0 WHERE id = ?",
                (valor_limpio, id_)
            )
            modificadas += 1

    conn.commit()
    return modificadas


# ─────────────────────────────────────────────────────────────────────────────
# PASO 2: DETECTAR VALORES NUEVOS SIN REGLA
# ─────────────────────────────────────────────────────────────────────────────

def detectar_sin_regla(conn, reglas):
    """
    Busca valores únicos en reunion_con que no tienen regla en el diccionario.
    Devuelve lista de (valor, n_veces) ordenada por frecuencia descendente.
    """
    cur = conn.cursor()
    filas = cur.execute(
        """
        SELECT reunion_con, COUNT(*) n
        FROM reuniones
        WHERE reunion_con IS NOT NULL AND normalizado = 0
        GROUP BY reunion_con
        ORDER BY n DESC
        """
    ).fetchall()

    sin_regla = []
    for valor, n in filas:
        if valor and valor.lower() not in reglas:
            sin_regla.append((valor, n))

    return sin_regla


# ─────────────────────────────────────────────────────────────────────────────
# PASO 3: GENERAR revisar.csv
# ─────────────────────────────────────────────────────────────────────────────

def generar_revisar(sin_regla, reglas, path_revisar):
    """
    Genera revisar.csv con:
    - Los valores sin regla
    - Una sugerencia fuzzy si hay alguna con score >= UMBRAL_FUZZY
    - Una columna vacía 'tu_decision' para que el usuario rellene
    """
    # Lista de canónicos conocidos para comparar
    canonicos_conocidos = list(set(v[0] for v in reglas.values()))

    filas_revisar = []
    for valor, n in sin_regla:
        sugerencia, score = buscar_mejor_sugerencia(valor, canonicos_conocidos)
        filas_revisar.append({
            "valor_original": valor,
            "veces_en_bd":    n,
            "sugerencia":     sugerencia or "",
            "similitud_pct":  score if sugerencia else "",
            "tu_decision":    "",   # ← el usuario rellena esto
            "sector":         "",   # ← opcional, el usuario puede añadirlo
        })

    with open(path_revisar, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, delimiter=SEPARADOR_CSV,
                                fieldnames=["valor_original", "veces_en_bd",
                                            "sugerencia", "similitud_pct",
                                            "tu_decision", "sector"])
        writer.writeheader()
        writer.writerows(filas_revisar)

    log(f"📋 {len(filas_revisar)} valores sin normalizar → {path_revisar}")
    con_sugerencia = sum(1 for f in filas_revisar if f["sugerencia"])
    log(f"   {con_sugerencia} tienen sugerencia automática ({UMBRAL_FUZZY}%+ similitud)")
    log(f"   {len(filas_revisar) - con_sugerencia} no tienen sugerencia — revisión manual")


# ─────────────────────────────────────────────────────────────────────────────
# PASO 4: LEER DECISIONES DEL USUARIO Y ACTUALIZAR reglas.csv
# ─────────────────────────────────────────────────────────────────────────────

def incorporar_decisiones(path_revisar, path_reglas, reglas):
    """
    Lee revisar.csv buscando filas donde el usuario haya rellenado 'tu_decision'.
    - Si puso un nombre canónico → añade la regla a reglas.csv
    - Si puso 'OK' → marca el valor como ya correcto (se añade como regla identidad)
    - Si dejó vacío → lo ignora (seguirá en revisar.csv la próxima semana)

    Devuelve el número de nuevas reglas incorporadas.
    """
    if not os.path.exists(path_revisar):
        return 0

    nuevas = 0
    with open(path_revisar, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=SEPARADOR_CSV)
        for fila in reader:
            decision = fila.get("tu_decision", "").strip()
            if not decision:
                continue  # sin decisión → saltar

            valor_original = fila["valor_original"].strip()
            sector         = fila.get("sector", "").strip()

            if decision.upper() == "OK":
                # El valor ya está bien tal cual
                canonico = valor_original
            else:
                canonico = decision

            clave = valor_original.lower()
            if clave not in reglas:
                reglas[clave] = (canonico, sector)
                nuevas += 1
                log(f"  + Nueva regla: '{valor_original}' → '{canonico}'")

    if nuevas:
        guardar_reglas(path_reglas, reglas)
        log(f"✅ {nuevas} nuevas reglas incorporadas a {path_reglas}")
    else:
        log("ℹ️  No hay decisiones nuevas en revisar.csv")

    return nuevas


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Normaliza reunion_con en la BD de MEPs")
    parser.add_argument("--db",           default=DEFAULT_DB,      help="Ruta a la BD SQLite")
    parser.add_argument("--reglas",       default=DEFAULT_REGLAS,  help="Ruta a reglas.csv")
    parser.add_argument("--revisar",      default=DEFAULT_REVISAR, help="Ruta a revisar.csv")
    parser.add_argument("--solo-revisar", action="store_true",     help="Solo genera revisar.csv sin modificar la BD")
    args = parser.parse_args()

    log("═" * 60)
    log("NORMALIZADOR DE REUNIONES MEPs")
    log("═" * 60)

    # ── Verificar que existe la BD ──────────────────────────────────────────
    if not os.path.exists(args.db):
        log(f"❌ No se encuentra la BD: {args.db}")
        log("   Asegúrate de estar en la carpeta correcta o usa --db para indicar la ruta")
        return

    conn = sqlite3.connect(args.db)

    # ── Cargar reglas ────────────────────────────────────────────────────────
    reglas = leer_reglas(args.reglas)

    # ── Incorporar decisiones previas del usuario ────────────────────────────
    log("\n[1/4] Incorporando tus decisiones anteriores de revisar.csv...")
    incorporar_decisiones(args.revisar, args.reglas, reglas)

    # Recargar reglas por si se añadieron nuevas
    reglas = leer_reglas(args.reglas)

    if not args.solo_revisar:
        # ── Aplicar reglas a la BD ───────────────────────────────────────────
        log("\n[2/4] Aplicando reglas a la BD...")
        modificadas = aplicar_reglas(conn, reglas)
        log(f"   {modificadas} filas modificadas")
    else:
        log("\n[2/4] Modo --solo-revisar: se omite la modificación de la BD")

    # ── Detectar sin regla ────────────────────────────────────────────────────
    log("\n[3/4] Detectando valores sin normalizar...")
    sin_regla = detectar_sin_regla(conn, reglas)
    log(f"   {len(sin_regla)} valores únicos sin regla")

    # ── Generar revisar.csv ───────────────────────────────────────────────────
    log("\n[4/4] Generando revisar.csv...")
    if sin_regla:
        generar_revisar(sin_regla, reglas, args.revisar)
    else:
        log("   🎉 ¡Todo normalizado! No hay valores pendientes de revisar")

    conn.close()

    log("\n" + "═" * 60)
    log("RESUMEN")
    log("═" * 60)
    if sin_regla:
        log(f"  • Abre {args.revisar} en Excel")
        log(f"  • Rellena la columna 'tu_decision' con el nombre canónico")
        log(f"  • Escribe OK si el valor ya está bien tal cual")
        log(f"  • Deja vacío si no sabes (lo verás otra semana)")
        log(f"  • Vuelve a correr: python normalizar.py")
    log("═" * 60)


if __name__ == "__main__":
    main()
