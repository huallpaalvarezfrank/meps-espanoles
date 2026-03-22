# reuniones_meps_sqlite.py — v3 (selectores actualizados marzo 2026)
# Igual que reuniones_meps.py pero guarda en SQLite con UPSERT
# (no duplica reuniones si se ejecuta varias veces)
#
# Instalación:
#   python -m pip install playwright pandas openpyxl
#   python -m playwright install chromium
#
# Uso:
#   python reuniones_meps_sqlite.py                    # todos los MEPs
#   python reuniones_meps_sqlite.py --limit-meps 2     # prueba rápida
#   python reuniones_meps_sqlite.py --headless         # sin ventana
#   python reuniones_meps_sqlite.py --db mireuniones.db

import os
import re
import sys
import sqlite3
import argparse
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, Error as PWError

LIST_URL   = "https://www.europarl.europa.eu/meps/es/search/advanced?countryCode=ES"
_HERE      = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(_HERE, "meps_es_reuniones.db")

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS meps (
    mep_id          TEXT PRIMARY KEY,
    nombre          TEXT,
    foto_url        TEXT,
    grupo_politico  TEXT,
    rol_grupo       TEXT,
    partido_nacional TEXT
);

CREATE TABLE IF NOT EXISTS reuniones (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mep_id          TEXT,
    titulo          TEXT,
    fecha           TEXT,
    lugar           TEXT,
    en_su_calidad   TEXT,
    comision_codigo TEXT,
    reunion_con     TEXT,
    FOREIGN KEY (mep_id) REFERENCES meps(mep_id),
    UNIQUE(mep_id, titulo, fecha, reunion_con)  -- evita duplicados
);

CREATE INDEX IF NOT EXISTS idx_reuniones_mep    ON reuniones(mep_id);
CREATE INDEX IF NOT EXISTS idx_reuniones_fecha  ON reuniones(fecha);
CREATE INDEX IF NOT EXISTS idx_reuniones_con    ON reuniones(reunion_con);
"""


# ─────────────────────────────── Utilidades ───────────────────────────────────

def norm_text(t):
    return " ".join((t or "").split())

def absolutize_url(u: str):
    if not u:
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://www.europarl.europa.eu" + u
    return u

def get_mep_id_from_url(url: str):
    try:
        path = urlparse(url).path
        m = re.search(r"/meps/[a-z]{2}/(\d+)", path, re.IGNORECASE)
        return m.group(1) if m else None
    except Exception:
        return None

def accept_cookies_if_any(page):
    for sel in [
        "button:has-text('Aceptar todas las cookies')",
        "button:has-text('Aceptar todo')",
        "button:has-text('Aceptar')",
        "button:has-text('Accept all cookies')",
        "button:has-text('I accept')",
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=1500):
                btn.click()
                page.wait_for_timeout(400)
                break
        except Exception:
            continue


# ───────────────────────────── Navegación robusta ─────────────────────────────

def goto_with_retries(page, url, max_tries=4, wait_until="domcontentloaded"):
    ctx = page.context
    for _ in range(max_tries):
        try:
            page.goto(url, wait_until=wait_until)
            return page
        except PWError:
            try:
                page.wait_for_timeout(800)
                page.goto("about:blank", wait_until="domcontentloaded")
            except Exception:
                pass
            try:
                page.close()
            except Exception:
                pass
            page = ctx.new_page()
    page.goto(url, wait_until=wait_until)
    return page


# ──────────────────────────────── Foto del MEP ────────────────────────────────

def get_photo_url(page, mep_id):
    for sel in [
        'es-ratio img[src*="mepphoto"]',
        '.es_image-frame img[src*="mepphoto"]',
        '.erpl_image-frame img[src*="mepphoto"]',
        'img[src*="mepphoto"]',
    ]:
        try:
            el = page.locator(sel).first
            if el.count():
                src = absolutize_url(el.get_attribute("src"))
                if src:
                    return src
        except Exception:
            continue
    try:
        og = page.locator('meta[property="og:image"]').first
        if og.count():
            content = absolutize_url(og.get_attribute("content"))
            if content and "mepphoto" in content:
                return content
    except Exception:
        pass
    if mep_id:
        return f"https://www.europarl.europa.eu/mepphoto/{mep_id}.jpg"
    return None


# ───────────────────────────── Metadatos del MEP ──────────────────────────────

def extract_mep_meta(page, mep_id):
    mep_name = None
    for sel in [".sln-member-name", "h1.es_title-h1"]:
        try:
            el = page.locator(sel).first
            if el.count():
                mep_name = norm_text(el.inner_text())
                break
        except Exception:
            continue

    photo = get_photo_url(page, mep_id)

    grupo_politico = None
    for sel in [".sln-political-group-name", "h3.sln-political-group-name"]:
        try:
            el = page.locator(sel).first
            if el.count():
                grupo_politico = norm_text(el.inner_text())
                break
        except Exception:
            continue

    rol_grupo = None
    for sel in [".sln-political-group-role", "p.sln-political-group-role"]:
        try:
            el = page.locator(sel).first
            if el.count():
                rol_grupo = norm_text(el.inner_text())
                break
        except Exception:
            continue

    partido_nacional = None
    for sel in ["div.es_title-h3.mt-1.mb-1", "div.es_title-h3", "div.erpl_title-h3.mt-1.mb-1", "div.erpl_title-h3"]:
        try:
            el = page.locator(sel).first
            if el.count():
                txt = norm_text(el.inner_text())
                if " - " in txt:
                    _, partido_nacional = txt.split(" - ", 1)
                    partido_nacional = norm_text(partido_nacional)
                elif txt:
                    partido_nacional = txt
                break
        except Exception:
            continue

    return {
        "mep_id": mep_id,
        "nombre": mep_name,
        "foto_url": photo,
        "grupo_politico": grupo_politico,
        "rol_grupo": rol_grupo,
        "partido_nacional": partido_nacional,
    }


# ──────────────────────────── Navegar a /meetings/past ────────────────────────

def goto_meetings_past(page):
    accept_cookies_if_any(page)
    if re.search(r"/meetings/past", page.url):
        return page
    base = re.sub(
        r"/(home|meetings(?:/past)?|activities|cv|declarations|assistants|history|main-activities|other-activities).*$",
        "", page.url
    ).rstrip("/")
    target = base + "/meetings/past#detailedcardmep"
    page = goto_with_retries(page, target)
    accept_cookies_if_any(page)
    return page


# ──────────────────────── Cargar todos los contenidos ─────────────────────────

def load_all_meetings(page):
    no_growth = 0
    while True:
        current = page.locator(".es_document").count()

        btn = page.locator("button.europarl-expandable-async-loadmore").first
        if not btn.count() or not btn.is_visible(timeout=1000):
            btn = page.get_by_role("button", name=re.compile(r"cargue otros contenidos", re.IGNORECASE)).first

        if not btn.count() or not btn.is_visible(timeout=1000):
            break

        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(400)
            try:
                btn.click(timeout=8000)
            except Exception:
                try:
                    handle = btn.element_handle(timeout=1000)
                    if handle:
                        page.evaluate("(el) => el.click()", handle)
                except Exception:
                    pass
            page.wait_for_timeout(1500)
            new_count = page.locator(".es_document").count()
            if new_count <= current:
                no_growth += 1
                if no_growth >= 3:
                    break
            else:
                no_growth = 0
        except Exception:
            no_growth += 1
            if no_growth >= 3:
                break


# ─────────────────────────────── Extraer reuniones ────────────────────────────

def extract_meetings(page, mep_id):
    rows = []
    for c in page.query_selector_all(".es_document"):
        title_el = c.query_selector(".es_document-title .t-item") or c.query_selector(".es_document-title")
        title = norm_text(title_el.inner_text()) if title_el else None

        t = c.query_selector(".es_document-subtitle-date time")
        fecha = t.get_attribute("datetime") if t else None
        if not fecha and t:
            fecha = norm_text(t.inner_text())

        lugar_el = c.query_selector(".es_document-subtitle-location")
        lugar = norm_text(lugar_el.inner_text()) if lugar_el else None

        cap_el = c.query_selector(".es_document-subtitle-capacity")
        en_calidad = norm_text(cap_el.inner_text()) if cap_el else None

        codes = [norm_text(b.inner_text()) for b in c.query_selector_all(".es_badge.es_badge-committee")]

        author_el = c.query_selector(".es_document-subtitle-author")
        reunion_con = norm_text(author_el.inner_text()) if author_el else None

        rows.append({
            "mep_id": mep_id,
            "titulo": title,
            "fecha": fecha,
            "lugar": lugar,
            "en_su_calidad": en_calidad,
            "comision_codigo": "|".join(codes) if codes else None,
            "reunion_con": reunion_con,
        })
    return rows


# ──────────────────────────── Abrir nueva pestaña ────────────────────────────

def open_in_new_tab(context, link_el):
    modifiers = ["Meta"] if sys.platform == "darwin" else ["Control"]
    with context.expect_page() as info:
        link_el.click(button="left", modifiers=modifiers, timeout=15000)
    new_page = info.value
    new_page.set_viewport_size({"width": 1366, "height": 900})
    new_page.wait_for_load_state("domcontentloaded")
    accept_cookies_if_any(new_page)
    return new_page


# ──────────────────────────────── SQLite ─────────────────────────────────────

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.executescript(DB_SCHEMA)
    conn.commit()
    return conn

def upsert_mep(conn, meta):
    conn.execute("""
        INSERT INTO meps (mep_id, nombre, foto_url, grupo_politico, rol_grupo, partido_nacional)
        VALUES (:mep_id, :nombre, :foto_url, :grupo_politico, :rol_grupo, :partido_nacional)
        ON CONFLICT(mep_id) DO UPDATE SET
            nombre           = excluded.nombre,
            foto_url         = excluded.foto_url,
            grupo_politico   = excluded.grupo_politico,
            rol_grupo        = excluded.rol_grupo,
            partido_nacional = excluded.partido_nacional
    """, meta)

def insert_reuniones(conn, rows):
    inserted = 0
    for r in rows:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO reuniones
                    (mep_id, titulo, fecha, lugar, en_su_calidad, comision_codigo, reunion_con)
                VALUES
                    (:mep_id, :titulo, :fecha, :lugar, :en_su_calidad, :comision_codigo, :reunion_con)
            """, r)
            inserted += conn.execute("SELECT changes()").fetchone()[0]
        except Exception:
            pass
    conn.commit()
    return inserted


# ─────────────────────────────────── Main ─────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="Scraper reuniones eurodiputados → SQLite")
    ap.add_argument("--db", default=DEFAULT_DB, help="Archivo SQLite de salida")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--limit-meps", type=int, default=0)
    return ap.parse_args()


def main():
    args = parse_args()
    conn = init_db(args.db)
    total_new = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context()
        context.set_default_timeout(45000)
        context.set_default_navigation_timeout(90000)

        # ── Listado de MEPs ───────────────────────────────────────────────────
        page = context.new_page()
        page = goto_with_retries(page, LIST_URL)
        accept_cookies_if_any(page)

        try:
            page.wait_for_selector("a.es_member-list-item-content", timeout=15000)
        except Exception:
            pass

        meps = []
        for a in page.query_selector_all("a.es_member-list-item-content"):
            href = a.get_attribute("href") or ""
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.europarl.europa.eu" + href
            name_el = (
                a.query_selector(".es_title-h4.t-item")
                or a.query_selector(".erpl_title-h4.t-item")
                or a.query_selector(".t-item")
            )
            name = norm_text(name_el.inner_text()) if name_el else None
            mep_id = get_mep_id_from_url(href)
            if name and mep_id:
                meps.append({"name": name, "href": href, "anchor": a, "mep_id": mep_id})

        if args.limit_meps:
            meps = meps[:args.limit_meps]

        print(f"MEPs encontrados: {len(meps)}")

        # ── Por cada MEP ──────────────────────────────────────────────────────
        for i, mep in enumerate(meps, 1):
            print(f"[{i}/{len(meps)}] {mep['name']}", end="", flush=True)
            try:
                mep_page = open_in_new_tab(context, mep["anchor"])
            except Exception:
                mep_page = context.new_page()
                mep_page = goto_with_retries(mep_page, mep["href"])

            mep_id = mep["mep_id"]

            # Metadatos
            meta = extract_mep_meta(mep_page, mep_id)
            if not meta.get("foto_url"):
                meta["foto_url"] = f"https://www.europarl.europa.eu/mepphoto/{mep_id}.jpg"
            upsert_mep(conn, meta)

            # Reuniones
            mep_page = goto_meetings_past(mep_page)
            load_all_meetings(mep_page)
            rows = extract_meetings(mep_page, mep_id)
            new = insert_reuniones(conn, rows)
            total_new += new
            print(f"  → {len(rows)} reuniones ({new} nuevas)")

            mep_page.close()

        browser.close()

    conn.close()

    print(f"\n✅ Base de datos: {args.db}")
    print(f"   Nuevas reuniones insertadas: {total_new}")
    print(f"\nPara explorar los datos:")
    print(f"   datasette {args.db}")


if __name__ == "__main__":
    main()