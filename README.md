# Eurodiputómetro · MEPs España

[![Licencia: CC BY-NC 4.0](https://img.shields.io/badge/Licencia-CC%20BY--NC%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc/4.0/)
[![GitHub Pages](https://img.shields.io/badge/Hosting-GitHub%20Pages-blue)](https://huallpaalvarezfrank.github.io/meps-espanoles/)

Portal de transparencia parlamentaria que recoge y publica las **reuniones de los 60 eurodiputados españoles** con empresas, organizaciones e instituciones. Los datos provienen del [Registro de Transparencia del Parlamento Europeo](https://www.europarl.europa.eu/meps/es/search/advanced?countryCode=ES) y se actualizan automáticamente cada semana.

**Demo:** [huallpaalvarezfrank.github.io/meps-espanoles](https://huallpaalvarezfrank.github.io/meps-espanoles/)

---

## Estructura del proyecto

```
meps-espanoles/
├── index.html                  ← Portal web completo (HTML + CSS + JS, sin backend)
├── 404.html                    ← Página de error personalizada
├── meps_es_reuniones.db        ← Base de datos SQLite (cargada en el navegador via sql.js)
├── robots.txt
├── sitemap.xml
├── .github/
│   ├── workflows/
│   │   └── pipeline.yml        ← CI/CD semanal (GitHub Actions)
│   └── ISSUE_TEMPLATE/
│       └── bug_report.md
└── etl/                        ← Scripts de extracción, transformación y carga
    ├── EU.py                   ← Scraper principal (Playwright)
    ├── clasificar.py           ← Clasificación con IA (Claude API)
    ├── normalizar.py           ← Normalización de nombres con reglas.csv
    ├── fix_sectores.py         ← Correcciones de sectores
    ├── fix_sectors.py
    ├── limpiar_sectores.py
    ├── reglas.csv              ← Diccionario de equivalencias editable
    ├── correcciones.csv        ← Correcciones manuales
    └── revisar.csv             ← Valores pendientes de revisión (generado por normalizar.py)
```

---

## Pipeline ETL

### Requisitos

```bash
pip install playwright pandas openpyxl anthropic
python -m playwright install chromium
```

### Ejecutar manualmente

```bash
# 1. Scraping de los 60 MEPs (usa --limit-meps N para pruebas)
python etl/EU.py --headless

# 2. Clasificar reuniones nuevas con IA (requiere ANTHROPIC_API_KEY)
export ANTHROPIC_API_KEY=sk-...
python -X utf8 etl/clasificar.py

# 3. Normalizar nombres de organizaciones
python -X utf8 etl/normalizar.py

# 4. Revisar etl/revisar.csv manualmente y volver a ejecutar normalizar.py
```

### Automatización (GitHub Actions)

El pipeline se ejecuta automáticamente cada **domingo a las 23:59** (hora española) via `.github/workflows/pipeline.yml`. Requiere el secreto `ANTHROPIC_API_KEY` configurado en el repositorio.

---

## Datos

- **Fuente:** Parlamento Europeo — [europarl.europa.eu](https://www.europarl.europa.eu)
- **Cobertura:** 60 eurodiputados españoles, reuniones desde 2019
- **Actualización:** Semanal (cada domingo)
- **Formato:** SQLite — tabla `reuniones` + tabla `meps`

---

## Autor

**Frank Huallpa Alvarez** — Politólogo, máster en Estrategias y Tecnologías para el Desarrollo (UCM-UPM)

Licencia: [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) · © 2026
