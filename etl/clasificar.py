"""
clasificar.py
─────────────
Clasifica las reuniones de la BD usando la API de Claude.
Extrae actores, sector y tipo de reunión para cada registro.

Uso:
    python clasificar.py                    # clasifica todos los pendientes
    python clasificar.py --db mi_bd.db      # BD personalizada
    python clasificar.py --reglas reglas.csv
    python clasificar.py --batch-size 10    # lotes más pequeños (por defecto: 20)
    python clasificar.py --limit 50         # procesar solo N registros (para pruebas)
    python clasificar.py --dry-run          # muestra resultados sin escribir en la BD

Requiere:
    ANTHROPIC_API_KEY en variable de entorno
    pip install anthropic
"""

import sqlite3
import csv
import os
import json
import time
import argparse
from datetime import datetime

import anthropic

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

_HERE           = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB      = os.path.join(_HERE, "..", "meps_es_reuniones.db")
DEFAULT_REGLAS  = os.path.join(_HERE, "reglas.csv")
BATCH_SIZE      = 20
MODEL           = "claude-sonnet-4-6"
MAX_TOKENS      = 4096
SEPARADOR_CSV   = ";"

VALID_SECTORS = [
    "Energía", "Banca", "Tecnología", "ONG", "Transporte", "Agricultura",
    "Salud", "Medio ambiente", "Industria", "Educación", "Medios", "Defensa",
    "Consultoría", "Evento", "Institución pública", "Institución UE",
    "Organizaciones empresariales", "Sindicatos", "Think tank",
    "Automoción", "Tabaco", "Distribución", "Deporte", "Cosméticos y perfumería",
    "Turismo", "Otros",
]

# Mapa de normalización: si reglas.csv o Claude devuelven un sector no canónico,
# se reemplaza por su equivalente canónico antes de escribir en la BD.
SECTOR_NORMALIZE = {
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

SYSTEM_PROMPT = f"""Eres un asistente especializado en transparencia política del Parlamento Europeo.

Tu tarea: dado un array JSON de reuniones de eurodiputados españoles, extrae para cada reunión:

1. actores: nombres canónicos de las entidades (organizaciones, empresas, personas, instituciones) separados por |
   - Siempre rellena este campo aunque solo haya un actor
   - No incluyas al eurodiputado (él es el anfitrión, no el actor externo)
   - Usa el nombre más reconocible en su idioma original (ej: "Apple Inc.", no "Apple Incorporated")
   - Usa el contexto del título de la reunión para desambiguar si es necesario
   - Para entidades españolas o europeas usa SIEMPRE el nombre en español:
     "Gobierno de España" (no "Government of Spain"), "Comisión Europea" (no "European Commission"),
     "Parlamento Europeo" (no "European Parliament"), "Consejo de la Unión Europea" (no "Council of the EU"),
     "Representación Permanente de España ante la UE" (no "Spanish Permanent Representation")
   - Para entidades de otros países usa su nombre en idioma original: "Deutsche Bank", "Renault S.A."
   - Usa la forma legal completa cuando sea conocida: "CaixaBank, S.A." no "CaixaBank"

2. sector: elige EXACTAMENTE una de estas categorías (copia el texto exacto):
   {", ".join(VALID_SECTORS)}

3. tipo_reunion: "Individual" si hay exactamente 1 actor, "Grupal" si hay 2 o más

Responde ÚNICAMENTE con JSON válido, sin markdown, sin explicaciones adicionales:
{{"resultados": [{{"id": <int>, "actores": "<str>", "sector": "<str>", "tipo_reunion": "<str>"}}, ...]}}"""


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def leer_reglas(path_reglas):
    """
    Lee reglas.csv y devuelve un diccionario:
        { valor_original_en_minusculas: (nombre_canonico, sector) }
    """
    reglas = {}
    if not os.path.exists(path_reglas):
        log(f"⚠️  No se encontró {path_reglas} — se continúa sin reglas manuales")
        return reglas

    with open(path_reglas, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=SEPARADOR_CSV)
        for fila in reader:
            clave   = fila["valor_original"].strip().lower()
            sector  = fila.get("sector", "").strip()
            if clave and sector:
                reglas[clave] = sector

    log(f"📖 {len(reglas)} reglas con sector cargadas desde {path_reglas}")
    return reglas


# ─────────────────────────────────────────────────────────────────────────────
# MIGRACIÓN: añadir columnas si no existen
# ─────────────────────────────────────────────────────────────────────────────

def add_missing_columns(conn):
    """Añade columnas nuevas a la tabla reuniones si no existen (igual que normalizar.py)."""
    cur = conn.cursor()
    columnas = [r[1] for r in cur.execute("PRAGMA table_info(reuniones)").fetchall()]

    nuevas = [
        ("reunion_con_original", "TEXT"),
        ("actores",              "TEXT"),
        ("sector",               "TEXT"),
        ("tipo_reunion",         "TEXT"),
        ("normalizado",          "INTEGER DEFAULT 0"),
    ]
    for nombre, tipo in nuevas:
        if nombre not in columnas:
            cur.execute(f"ALTER TABLE reuniones ADD COLUMN {nombre} {tipo}")
            log(f"  → Columna {nombre} creada")

    # Backup de valores originales (solo la primera vez)
    cur.execute(
        "UPDATE reuniones SET reunion_con_original = reunion_con "
        "WHERE reunion_con_original IS NULL AND reunion_con IS NOT NULL"
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# FETCH DE REGISTROS SIN CLASIFICAR
# ─────────────────────────────────────────────────────────────────────────────

def fetch_unclassified(conn, limit=None):
    """
    Devuelve registros donde actores IS NULL.
    Usa reunion_con_original si existe, si no cae back a reunion_con (columna cruda).
    """
    cur = conn.cursor()
    sql = """
        SELECT
            id,
            COALESCE(reunion_con_original, reunion_con) AS texto,
            titulo
        FROM reuniones
        WHERE actores IS NULL
          AND COALESCE(reunion_con_original, reunion_con) IS NOT NULL
        ORDER BY id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    return [{"id": r[0], "texto": r[1], "titulo": r[2] or ""} for r in cur.execute(sql).fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# LLAMADA A CLAUDE
# ─────────────────────────────────────────────────────────────────────────────

def build_user_message(batch):
    """Construye el array JSON que se envía a Claude."""
    items = [
        {"id": r["id"], "reunion_con": r["texto"], "titulo": r["titulo"]}
        for r in batch
    ]
    return json.dumps(items, ensure_ascii=False)


def call_claude(client, batch, max_retries=3):
    """Llama a la API de Claude con reintentos. Devuelve el texto de la respuesta o None."""
    user_msg = build_user_message(batch)
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}],
            )
            return response.content[0].text
        except anthropic.RateLimitError:
            wait = 60 * (attempt + 1)
            log(f"  ⏳ Rate limit alcanzado, esperando {wait}s (intento {attempt + 1}/{max_retries})...")
            time.sleep(wait)
        except anthropic.APIStatusError as e:
            if attempt == max_retries - 1:
                log(f"  ❌ Error API tras {max_retries} intentos: {e.status_code} {e.message}")
                return None
            time.sleep(5 * (attempt + 1))
        except anthropic.APIError as e:
            if attempt == max_retries - 1:
                log(f"  ❌ Error API inesperado: {e}")
                return None
            time.sleep(5 * (attempt + 1))
    return None


def parse_response(raw_text, batch_ids):
    """
    Parsea la respuesta JSON de Claude.
    Devuelve dict: { id -> {"actores": str, "sector": str, "tipo_reunion": str} }
    Los IDs que no aparezcan en la respuesta quedan sin clasificar (actores IS NULL).
    """
    if not raw_text:
        return {}
    try:
        # Quitar markdown si Claude lo añade a pesar de la instrucción
        text = raw_text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:]).rstrip("`").strip()

        data = json.loads(text)
        results = {}
        for item in data.get("resultados", []):
            rid = item.get("id")
            if rid in batch_ids:
                actores      = str(item.get("actores", "")).strip()
                sector       = str(item.get("sector", "Otros")).strip()
                tipo_reunion = str(item.get("tipo_reunion", "Individual")).strip()

                # Normalizar sector (elimina variantes no canónicas)
                sector = SECTOR_NORMALIZE.get(sector, sector)
                if sector not in VALID_SECTORS:
                    sector = "Otros"
                # Validar tipo_reunion
                if tipo_reunion not in ("Individual", "Grupal"):
                    tipo_reunion = "Individual" if "|" not in actores else "Grupal"

                results[rid] = {
                    "actores":      actores,
                    "sector":       sector,
                    "tipo_reunion": tipo_reunion,
                }
        return results

    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log(f"  ❌ Error parseando respuesta: {e}")
        log(f"     Raw (primeros 300 chars): {raw_text[:300]}")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# ESCRITURA EN BD
# ─────────────────────────────────────────────────────────────────────────────

def write_results(conn, results, reglas_sector, batch_records):
    """
    Escribe los resultados de clasificación en la BD.
    reglas_sector: { reunion_con.lower() -> sector } — sectores de reglas.csv tienen prioridad.
    """
    cur = conn.cursor()
    # Mapa id -> texto original para buscar en reglas_sector
    texto_por_id = {r["id"]: r["texto"] for r in batch_records}

    for rid, fields in results.items():
        # El sector de reglas.csv tiene prioridad sobre el de Claude
        texto_original = texto_por_id.get(rid, "")
        sector_manual  = reglas_sector.get(texto_original.lower(), "")
        # Normalizar sector de reglas.csv también (puede tener variantes antiguas)
        if sector_manual:
            sector_manual = SECTOR_NORMALIZE.get(sector_manual, sector_manual)
            if sector_manual not in VALID_SECTORS:
                sector_manual = ""
        sector_final   = sector_manual if sector_manual else fields["sector"]

        cur.execute(
            "UPDATE reuniones SET actores = ?, sector = ?, tipo_reunion = ? WHERE id = ?",
            (fields["actores"], sector_final, fields["tipo_reunion"], rid),
        )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Clasifica reuniones de MEPs usando Claude API")
    parser.add_argument("--db",          default=DEFAULT_DB,    help="Ruta a la BD SQLite")
    parser.add_argument("--reglas",      default=DEFAULT_REGLAS, help="Ruta a reglas.csv")
    parser.add_argument("--batch-size",  type=int, default=BATCH_SIZE, help="Registros por llamada a Claude (por defecto: 20)")
    parser.add_argument("--limit",       type=int, default=None,  help="Máximo de registros a procesar (para pruebas)")
    parser.add_argument("--dry-run",     action="store_true",    help="Muestra resultados sin escribir en la BD")
    args = parser.parse_args()

    log("═" * 60)
    log("CLASIFICADOR DE REUNIONES MEPs")
    log("═" * 60)

    # ── Verificar API key ────────────────────────────────────────────────────
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log("❌ Variable de entorno ANTHROPIC_API_KEY no encontrada.")
        log("   Ejecútalo así: ANTHROPIC_API_KEY=sk-... python clasificar.py")
        return

    # ── Verificar BD ─────────────────────────────────────────────────────────
    if not os.path.exists(args.db):
        log(f"❌ No se encuentra la BD: {args.db}")
        log("   Asegúrate de estar en la carpeta correcta o usa --db para indicar la ruta")
        return

    conn = sqlite3.connect(args.db)
    client = anthropic.Anthropic(api_key=api_key)

    # ── Migración: añadir columnas si faltan ─────────────────────────────────
    log("\n[1/3] Comprobando esquema de la BD...")
    add_missing_columns(conn)

    # ── Cargar reglas manuales de sector ─────────────────────────────────────
    reglas_sector = leer_reglas(args.reglas)

    # ── Fetch registros sin clasificar ───────────────────────────────────────
    log("\n[2/3] Buscando reuniones sin clasificar...")
    records = fetch_unclassified(conn, limit=args.limit)
    total = len(records)

    if total == 0:
        log("✅ No hay reuniones pendientes de clasificar. ¡Todo al día!")
        conn.close()
        return

    log(f"   {total} reuniones a clasificar")
    if args.dry_run:
        log("   Modo --dry-run activado: los resultados NO se guardarán en la BD")

    # ── Clasificación por lotes ───────────────────────────────────────────────
    log(f"\n[3/3] Clasificando en lotes de {args.batch_size}...")
    batches     = list(chunked(records, args.batch_size))
    n_batches   = len(batches)
    clasificadas = 0
    fallidas     = 0

    for i, batch in enumerate(batches, 1):
        batch_ids = {r["id"] for r in batch}
        id_min, id_max = min(batch_ids), max(batch_ids)

        raw = call_claude(client, batch)
        results = parse_response(raw, batch_ids)

        n_ok   = len(results)
        n_fail = len(batch) - n_ok
        clasificadas += n_ok
        fallidas     += n_fail

        estado = "OK" if n_fail == 0 else f"⚠️  {n_fail} fallidas"
        log(f"  Lote {i}/{n_batches}: IDs {id_min}–{id_max} → {estado} ({n_ok} clasificadas)")

        if args.dry_run:
            for rid, fields in results.items():
                log(f"    [{rid}] actores={fields['actores']!r} | sector={fields['sector']!r} | tipo={fields['tipo_reunion']!r}")
        else:
            write_results(conn, results, reglas_sector, batch)

        # Pausa cortés entre lotes para no saturar la API
        if i < n_batches:
            time.sleep(0.5)

    conn.close()

    # ── Resumen ───────────────────────────────────────────────────────────────
    log("\n" + "═" * 60)
    log("RESUMEN")
    log("═" * 60)
    if args.dry_run:
        log(f"  • Modo dry-run: {clasificadas} habrían sido clasificadas, {fallidas} habrían fallado")
        log(f"  • La BD NO ha sido modificada")
    else:
        log(f"  • {clasificadas} reuniones clasificadas correctamente")
        if fallidas:
            log(f"  • {fallidas} registros fallaron y quedarán como actores IS NULL")
            log(f"    → Vuelve a ejecutar clasificar.py para reintentarlos")
        else:
            log(f"  • ✅ Sin errores")
    log("═" * 60)


if __name__ == "__main__":
    main()
