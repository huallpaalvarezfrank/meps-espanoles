# Proyecto: Portal de reuniones de eurodiputados españoles

## Descripción
Portal de transparencia política que recoge, limpia y publica las reuniones de los 60 eurodiputados españoles con empresas, organizaciones e instituciones. Los datos provienen del Parlamento Europeo (europarl.europa.eu). El objetivo final es un portal web público donde cualquier ciudadano pueda buscar con quién se reúnen los eurodiputados españoles.

## Estructura del proyecto
```
EU/
├── EU.py                    ← scraper principal (Playwright)
├── normalizar.py            ← normalizador de nombres con reglas.csv
├── reglas.csv               ← diccionario de equivalencias editable
├── revisar.csv              ← valores pendientes de revisión manual
├── meps_es_reuniones.db     ← base de datos SQLite actual
├── clasificar.py            ← (POR CREAR) clasificación automática con IA
└── CLAUDE.md                ← este archivo
```

## Estado actual del proyecto
- El scraper EU.py funciona y extrae reuniones de los 60 MEPs españoles
- La BD tiene ~4.000 registros de reuniones
- normalizar.py aplica reglas de limpieza y genera revisar.csv con pendientes
- Falta: script clasificar.py, migración al esquema nuevo, portal web

## Base de datos — esquema ACTUAL (meps_es_reuniones.db)

### Tabla `meps`
- mep_id, nombre, grupo, partido, foto_url, perfil_url, comisiones

### Tabla `reuniones`
- id, mep_id, titulo, fecha, lugar, comision, reunion_con
- reunion_con_original (valor bruto del scraper)

## Base de datos — esquema OBJETIVO (v2)

### Tabla `meps` (sin cambios)
- mep_id INTEGER PRIMARY KEY
- nombre TEXT
- partido_nacional TEXT        ← partido español (PP, PSOE, etc.)
- grupo_europeo TEXT           ← grupo en el PE (PPE, S&D, Renew, etc.)
- comisiones TEXT              ← separadas por |
- foto_url TEXT
- perfil_url TEXT

### Tabla `reuniones` (columnas nuevas marcadas con →)
- id INTEGER PRIMARY KEY
- mep_id INTEGER (FK → meps)
- titulo TEXT                  ← tema o título de la reunión
- fecha TEXT                   ← formato AAAA-MM-DD
- lugar TEXT
- comision TEXT                ← comisión del eurodiputado en esa reunión
- reunion_con_original TEXT    ← valor bruto tal como viene del scraper
- reunion_con_normalizado TEXT ← nombre limpio y canónico (de normalizar.py)
- → actores TEXT               ← entidades separadas por | generadas por IA
- → sector TEXT                ← ej. Energía, Banca, Tecnología, ONG
- → tipo_reunion TEXT          ← "Individual" o "Grupal"

## Diferencia entre columnas clave
- `reunion_con_original`: texto bruto del scraper, nunca se toca
- `reunion_con_normalizado`: texto limpio aplicando reglas.csv (ej. "MERCADONA SA" → "Mercadona, S.A.")
- `actores`: lista estructurada separada por | para búsqueda (ej. "Apple Inc.|Google LLC|Meta Platforms")
- `tipo_reunion`: Individual si hay 1 actor, Grupal si hay más de uno
- `actores` se rellena para TODAS las reuniones, no solo las grupales, para poder buscar siempre en una sola columna

## Pipeline semanal (objetivo)
```
Cada lunes (GitHub Actions):
  1. EU.py          → scraping de los 60 MEPs, inserta reuniones nuevas en la BD
  2. clasificar.py  → llama a API de Claude, rellena actores/sector/tipo_reunion en registros nuevos
  3. normalizar.py  → aplica reglas.csv, actualiza reunion_con_normalizado, genera revisar.csv
  4. commit automático → BD actualizada en GitHub
  5. GitHub Pages   → portal actualizado automáticamente via Datasette Lite
```

## Trabajo manual semanal (solo esto)
- Abrir revisar.csv en Excel
- Revisar valores nuevos sin regla (columna `tu_decision`)
- Guardar como CSV UTF-8
- Volver a correr normalizar.py
- Tiempo estimado: 5-10 minutos

## Decisiones de arquitectura tomadas
- **Hosting**: GitHub Pages (gratuito) con Datasette Lite
- **Dominio**: subdominio github.io gratuito (tuusuario.github.io/nombre-repo)
- **Automatización**: GitHub Actions con cron semanal (cada lunes)
- **Clasificación IA**: API de Claude (claude-sonnet-4-6) vía clasificar.py
- **BD**: SQLite (sin migrar a PostgreSQL hasta que escale)
- **Portal**: Datasette Lite en primera fase; Next.js + FastAPI si se necesita más personalización

## Scraper (EU.py) — cómo funciona
- Usa Playwright con Chromium
- URL base: https://www.europarl.europa.eu/meps/es/search/advanced?countryCode=ES
- Selector MEPs: `a.es_member-list-item-content` (antes era erpl_, cambió en rediseño web)
- Por cada MEP: navega a /meetings/past, pulsa `button.europarl-expandable-async-loadmore` hasta que desaparece
- Selectores de reuniones: `.es_document`, `.es_document-title`, `.es_document-subtitle-date`, etc.
- Guarda en SQLite con INSERT OR IGNORE para evitar duplicados
- Argumentos: `--limit-meps N` para pruebas, `--headless` para producción

## Normalización (normalizar.py + reglas.csv)
- reglas.csv tiene columnas: valor_original, nombre_canonico, sector
- revisar.csv tiene columnas: valor_original, veces_en_bd, sugerencia, similitud_pct, tu_decision, sector
- Fuzzy matching con difflib (incluido en Python, sin dependencias extra)
- Lógica: solo se normaliza cuando el campo contiene exactamente una entidad
- Reuniones con varias entidades juntas se dejan intactas en reunion_con_normalizado

## Scripts por crear
### clasificar.py
- Lee registros donde actores IS NULL (registros nuevos sin clasificar)
- Llama a API de Claude (claude-sonnet-4-6) con el texto de reunion_con_original
- Extrae: actores (separados por |), sector, tipo_reunion (Individual/Grupal)
- API key en variable de entorno ANTHROPIC_API_KEY, nunca en el código
- Procesa en lotes para eficiencia
- La primera ejecución procesa todos los ~4.000 registros históricos
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
# Prueba scraper con 2 MEPs
python EU.py --limit-meps 2

# Scraping completo sin ventana
python EU.py --headless

# Normalizar
python normalizar.py

# Clasificar (cuando esté creado)
python clasificar.py
```

## Contexto del creador
- Politólogo con máster en Estrategias y Tecnologías para el Desarrollo (UCM-UPM)
- Objetivo: portal de transparencia sobre lobbying en el PE español
- Audiencia primaria del portal: periodistas y ciudadanos
- Búsqueda principal esperada: por nombre de empresa/organización
