# Proyecto: Portal de reuniones de eurodiputados españoles

## Descripción
Portal de transparencia política que recoge, limpia y publica las reuniones de los 60 eurodiputados españoles con empresas, organizaciones e instituciones. Los datos provienen del Parlamento Europeo (europarl.europa.eu). El objetivo final es un portal web público donde cualquier ciudadano pueda buscar con quién se reúnen los eurodiputados españoles.

## Estructura del proyecto
```
EU/
├── index.html               ← portal web (sql.js + Chart.js, sin backend)
├── 404.html                 ← página de error personalizada
├── meps_es_reuniones.db     ← base de datos SQLite (en la raíz, no en etl/)
├── robots.txt
├── sitemap.xml
├── etl/
│   ├── EU.py                ← scraper principal (Playwright)
│   ├── clasificar.py        ← clasificación automática con IA (Claude API)
│   ├── normalizar.py        ← normalizador de nombres con reglas.csv
│   ├── reglas.csv           ← diccionario de equivalencias editable
│   ├── revisar.csv          ← valores pendientes de revisión manual
│   └── correcciones.csv     ← correcciones manuales
└── CLAUDE.md                ← este archivo
```

## Estado actual del proyecto
- El scraper etl/EU.py funciona y extrae reuniones de los 60 MEPs españoles
- La BD tiene ~4.000 registros de reuniones
- etl/clasificar.py clasifica reuniones con la API de Claude (actores, sector, tipo_reunion)
- etl/normalizar.py aplica reglas de limpieza y genera etl/revisar.csv con pendientes
- index.html es el portal web que lee la BD directamente en el navegador con sql.js

## Base de datos — esquema ACTUAL (meps_es_reuniones.db)

### Tabla `meps`
- mep_id TEXT PRIMARY KEY
- nombre TEXT
- foto_url TEXT
- grupo_politico TEXT          ← grupo europeo (PPE, S&D, Renew, etc.)
- rol_grupo TEXT
- partido_nacional TEXT        ← partido español (PP, PSOE, etc.)

### Tabla `reuniones`
- id INTEGER PRIMARY KEY AUTOINCREMENT
- mep_id TEXT (FK → meps)
- titulo TEXT
- fecha TEXT                   ← formato AAAA-MM-DD
- lugar TEXT
- en_su_calidad TEXT
- comision_codigo TEXT         ← códigos de comisión separados por |
- reunion_con TEXT             ← nombre limpio/normalizado (actualizado por normalizar.py)
- reunion_con_original TEXT    ← valor bruto tal como viene del scraper (añadido por normalizar.py/clasificar.py)
- actores TEXT                 ← entidades separadas por | generadas por clasificar.py
- sector TEXT                  ← ej. Energía, Banca, Tecnología, ONG
- tipo_reunion TEXT            ← "Individual" o "Grupal"
- normalizado INTEGER DEFAULT 0 ← 1 si reunion_con fue normalizado con regla de reglas.csv

## Diferencia entre columnas clave
- `reunion_con_original`: texto bruto del scraper, nunca se toca
- `reunion_con`: texto limpio aplicando reglas.csv (actualizado in-place por normalizar.py)
- `actores`: lista estructurada separada por | para búsqueda (ej. "Apple Inc.|Google LLC|Meta Platforms")
- `tipo_reunion`: Individual si hay 1 actor, Grupal si hay más de uno
- `actores` se rellena para TODAS las reuniones, no solo las grupales, para poder buscar siempre en una sola columna

## Pipeline semanal (objetivo)
```
Cada domingo a las 23:59 (GitHub Actions):
  1. etl/EU.py          → scraping de los 60 MEPs, inserta reuniones nuevas en la BD
  2. etl/clasificar.py  → llama a API de Claude, rellena actores/sector/tipo_reunion en registros nuevos
  3. etl/normalizar.py  → aplica reglas.csv, actualiza reunion_con, genera etl/revisar.csv
  4. commit automático → meps_es_reuniones.db + etl/revisar.csv actualizados en GitHub
  5. GitHub Pages   → portal actualizado automáticamente
```

## Trabajo manual semanal (solo esto)
- Abrir etl/revisar.csv en Excel
- Revisar valores nuevos sin regla (columna `tu_decision`)
- Guardar como CSV UTF-8
- Volver a correr etl/normalizar.py
- Tiempo estimado: 5-10 minutos

## Decisiones de arquitectura tomadas
- **Hosting**: GitHub Pages (gratuito) con Datasette Lite
- **Dominio**: subdominio github.io gratuito (tuusuario.github.io/nombre-repo)
- **Automatización**: GitHub Actions con cron semanal (cada domingo a las 23:59)
- **Clasificación IA**: API de Claude (claude-sonnet-4-6) vía clasificar.py
- **BD**: SQLite (sin migrar a PostgreSQL hasta que escale)
- **Portal**: Datasette Lite en primera fase; Next.js + FastAPI si se necesita más personalización

## Scraper (etl/EU.py) — cómo funciona
- Usa Playwright con Chromium
- URL base: https://www.europarl.europa.eu/meps/es/search/advanced?countryCode=ES
- Selector MEPs: `a.es_member-list-item-content` (antes era erpl_, cambió en rediseño web)
- Por cada MEP: navega a /meetings/past, pulsa `button.europarl-expandable-async-loadmore` hasta que desaparece
- Selectores de reuniones: `.es_document`, `.es_document-title`, `.es_document-subtitle-date`, etc.
- Guarda en SQLite con INSERT OR IGNORE para evitar duplicados
- Argumentos: `--limit-meps N` para pruebas, `--headless` para producción

## Normalización (etl/normalizar.py + etl/reglas.csv)
- reglas.csv tiene columnas: valor_original, nombre_canonico, sector
- revisar.csv tiene columnas: valor_original, veces_en_bd, sugerencia, similitud_pct, tu_decision, sector
- Fuzzy matching con difflib (incluido en Python, sin dependencias extra)
- Lógica: solo se normaliza cuando el campo contiene exactamente una entidad
- Reuniones con varias entidades juntas se dejan intactas en reunion_con

## Script etl/clasificar.py
- Lee registros donde actores IS NULL (registros nuevos sin clasificar)
- Llama a API de Claude (claude-sonnet-4-6) con el texto de reunion_con_original
- Extrae: actores (separados por |), sector, tipo_reunion (Individual/Grupal)
- API key en variable de entorno ANTHROPIC_API_KEY, nunca en el código
- Procesa en lotes (por defecto 20 registros por llamada)
- La primera ejecución procesa todos los registros históricos sin clasificar
- Las siguientes solo procesan los nuevos de esa semana

## Dependencias Python
```
playwright
pandas
openpyxl
anthropic     ← para clasificar.py
```

## Comandos útiles
```bash
# Prueba scraper con 2 MEPs (desde la raíz del proyecto)
python etl/EU.py --limit-meps 2

# Scraping completo sin ventana
python etl/EU.py --headless

# Normalizar
python -X utf8 etl/normalizar.py

# Clasificar (requiere ANTHROPIC_API_KEY)
python -X utf8 etl/clasificar.py
```

## Contexto del creador
- Politólogo con máster en Estrategias y Tecnologías para el Desarrollo (UCM-UPM)
- Objetivo: portal de transparencia sobre lobbying en el PE español
- Audiencia primaria del portal: periodistas y ciudadanos
- Búsqueda principal esperada: por nombre de empresa/organización
