"""
fix_sectores.py
───────────────
Corrige inconsistencias de sector por actor en la BD.
Basado en análisis de 195 actores que aparecen en >1 sector.

Uso:
    python fix_sectores.py              # aplica correcciones
    python fix_sectores.py --dry-run    # muestra cambios sin escribir
"""

import sqlite3
import argparse
from datetime import datetime

DEFAULT_DB = "meps_es_reuniones.db"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ─────────────────────────────────────────────────────────────────────────────
# MAPA ACTOR → SECTOR CORRECTO
# Criterio: primacía al sector más frecuente + lógica sectorial EU
# ─────────────────────────────────────────────────────────────────────────────
CORRECTIONS = {

    # ── AUTOMOCIÓN ─────────────────────────────────────────────────────────
    # Fabricantes de coches, camiones, furgonetas y sus proveedores/asociaciones
    'Volkswagen AG':                                    'Automoción',
    'Stellantis':                                       'Automoción',
    'Hyundai Motor Europe':                             'Automoción',
    'Ford Motor Company':                               'Automoción',
    'IVECO GROUP N.V.':                                 'Automoción',
    'DENSO International Europe':                       'Automoción',
    'FORVIA':                                           'Automoción',
    'HORSE':                                            'Automoción',
    'FEV Europe GmbH':                                  'Automoción',
    # Asociaciones sectoriales del automóvil
    'ACEA':                                             'Automoción',
    'AVERE - The European Association for Electromobility': 'Automoción',
    'CLEPA':                                            'Automoción',
    'CLEPA - European Association of Automotive Suppliers': 'Automoción',
    'VDA':                                              'Automoción',
    "Association des Constructeurs Européens d'Automobiles": 'Automoción',
    'ASOCIACIÓN ESPAÑOLA DE FABRICANTES DE AUTOMÓVILES Y CAMIONES': 'Automoción',
    'Asociación Española de Fabricantes de Automóviles y Camiones': 'Automoción',
    "European Tyre & Rubber Manufacturers' Association":        'Automoción',
    "European Tyre & Rubber Manufacturers' Association (ETRMA)":'Automoción',
    'ETRMA - European Tyre and Rubber Manufacturers Association':'Automoción',

    # ── TRANSPORTE ─────────────────────────────────────────────────────────
    # Sector transporte no-automoción: ferroviario, marítimo, movilidad, seguridad vial
    'UNIFE':                                            'Transporte',
    'Enterprise Mobility':                              'Transporte',
    'European Transport Safety Council':                'Transporte',
    'Fastned BV':                                       'Transporte',
    'Zunder':                                           'Transporte',
    'Help Flash':                                       'Transporte',
    'Global Business Travel Association':               'Transporte',
    'European Boating Industry':                        'Transporte',
    'Shared Mobility Europe':                           'Transporte',
    'The Walt Disney Company Benelux BVBA':             'Medios',  # aparece en Transporte por error

    # ── ENERGÍA ────────────────────────────────────────────────────────────
    'FuelsEurope':                                      'Energía',
    'BP':                                               'Energía',

    # ── INDUSTRIA ──────────────────────────────────────────────────────────
    # Industria manufacturera, química, materiales, alimentación, bienes de consumo
    'DuPont de Nemours International SARL':             'Industria',
    'Etex':                                             'Industria',
    'Prosegur Compañía de Seguridad':                   'Industria',
    'European Aluminium AISBL':                         'Industria',
    'Cobalt Institute':                                 'Industria',
    'European Chemical Industry Council':               'Industria',
    'EUROFER':                                          'Industria',
    'FEDEMCO':                                          'Industria',
    'ASINCA':                                           'Industria',
    'Construction Products Europe':                     'Industria',
    'Plastics Recyclers Europe':                        'Industria',
    'Smiths Group plc':                                 'Industria',
    'General Electric Company':                         'Industria',
    'IKEA':                                             'Industria',
    'Fire Safe Europe':                                 'Industria',
    'Funditec':                                         'Industria',
    'Considerate Pouchers':                             'Industria',
    'Asociación Española del Aluminio y Tratamientos de Superficie': 'Industria',
    'Asociación Española de Aluminio':                  'Industria',
    # Alimentación/bienes de consumo (no agricultura primaria)
    'Danone':                                           'Industria',
    'Unilever':                                         'Industria',
    'The Kraft Heinz Company':                          'Industria',
    'The Coca-Cola Company':                            'Industria',
    'Pernod Ricard':                                    'Industria',

    # ── DEFENSA ────────────────────────────────────────────────────────────
    'Airbus':                                           'Defensa',
    'Raytheon Technologies':                            'Defensa',
    'Indra':                                            'Defensa',

    # ── SALUD ──────────────────────────────────────────────────────────────
    'Bayer AG':                                         'Salud',
    'Medicines for Europe':                             'Salud',
    'Affordable Medicines Europe':                      'Salud',
    'European Federation of Pharmaceutical Industries and Associations': 'Salud',
    'Pharmaceutical Group of the European Union':       'Salud',
    'RPP Group':                                        'Salud',
    'Asociación Española de Quiropráctica':             'Salud',
    'European Brain Council':                           'Salud',
    'Farmaindustria':                                   'Salud',
    'European Migraine and Headache Alliance':          'Salud',
    'European Organisation for Rare Diseases':          'Salud',
    'SEDRA':                                            'Salud',
    'Asociación Española Contra el Cáncer':             'Salud',
    'Biocat, Fundació BioRegió de Catalunya':           'Salud',

    # ── TABACO ─────────────────────────────────────────────────────────────
    'Philip Morris International':                      'Tabaco',
    'British American Tobacco':                         'Tabaco',
    "World Vapers' Alliance":                           'Tabaco',

    # ── TECNOLOGÍA ─────────────────────────────────────────────────────────
    'Telefónica, S.A.':                                 'Tecnología',
    'Spotify':                                          'Tecnología',
    'DOT Europe':                                       'Tecnología',
    "European Telecommunications Network Operators' Association": 'Tecnología',
    'Airbnb':                                           'Tecnología',
    'Adigital':                                         'Tecnología',
    'Pan European Game Information':                    'Tecnología',
    'Asociación Española de Videojuegos (AEVI)':        'Tecnología',
    'Asociación Española de Videojuegos':               'Tecnología',
    'Asociación espaañola de Videojuegos (AEVI)':       'Tecnología',

    # ── CONSULTORÍA ────────────────────────────────────────────────────────
    'Acumen Public Affairs':                            'Consultoría',
    'Vinces Consulting':                                'Consultoría',
    'Servicios Integrados Lasker S.L.':                 'Consultoría',
    'Hanbury Strategy and Communications Limited':      'Consultoría',
    'FTI Consulting Belgium':                           'Consultoría',
    'Kreab':                                            'Consultoría',
    'EPPA SA':                                          'Consultoría',
    'Penta Group':                                      'Consultoría',
    'OIKOS':                                            'Think tank',

    # ── BANCA ──────────────────────────────────────────────────────────────
    'European Banking Federation':                      'Banca',
    'AFME':                                             'Banca',
    'AIMA':                                             'Banca',
    'FBF':                                              'Banca',
    'ISDA':                                             'Banca',
    'Association for Financial Markets in Europe':      'Banca',
    'German Banking Industry Committee':                'Banca',
    'European Payment Institutions Federation':         'Banca',
    'Visa Europe':                                      'Banca',

    # ── AGRICULTURA ────────────────────────────────────────────────────────
    # Producción agrícola, pesca, alimentación en origen, cooperativas agro
    'Grupo Vall Companys':                              'Agricultura',
    'Cooperativas Agro-alimentarias de España':         'Agricultura',
    'AEPLA':                                            'Agricultura',
    'ANFACO-CECOPESCA':                                 'Agricultura',
    'ANMUPESCA':                                        'Agricultura',
    'COAG':                                             'Agricultura',
    'PRODULCE':                                         'Agricultura',
    'AEFA':                                             'Agricultura',
    'ANAMAR':                                           'Agricultura',
    'Europeche':                                        'Agricultura',
    'CONESA':                                           'Agricultura',
    'CEPESCA':                                          'Agricultura',
    'Cepesca':                                          'Agricultura',
    'Biocontrol Coalition':                             'Agricultura',
    'FEDIOL':                                           'Agricultura',
    'FAPROMA':                                          'Agricultura',
    'Conxemar':                                         'Agricultura',
    'GreenLight Biosciences':                           'Agricultura',
    'ANEABE':                                           'Agricultura',
    'European Food Forum':                              'Agricultura',
    'Instituto Interamericano de Cooperación para la Agricultura': 'Institución pública',

    # ── ONG ────────────────────────────────────────────────────────────────
    # ONGs, sociedad civil, organizaciones internacionales humanitarias
    'Human Rights Watch':                               'ONG',
    'FEANTSA':                                          'ONG',
    'Housing Europe':                                   'ONG',
    'International Union of Tenants':                   'ONG',
    'European Youth Forum':                             'ONG',
    'ASJUBI40 - Jubilación Anticipada Sin Penalizar':   'ONG',
    'Young European Socialists':                        'ONG',
    'YouthProAktiv':                                    'ONG',
    'ACCEM':                                            'ONG',
    'UPLA':                                             'ONG',
    'Zero Port':                                        'ONG',
    'NCRAT (National Constitution Restoration Alliance for Transition)': 'ONG',
    'UNHCR':                                            'ONG',
    'UN Women':                                         'ONG',
    'World Food Programme':                             'ONG',
    'SGI Europe':                                       'ONG',
    'FACE':                                             'ONG',
    '5Rights Foundation':                               'ONG',
    'Euroconsumers':                                    'ONG',
    # ONGs ambientales (clasificadas a veces como "Medio ambiente", pero son ONGs)
    'European Environmental Bureau':                    'ONG',
    'Ecologistas en Acción':                            'ONG',
    'WWF European Policy Programme':                    'ONG',
    'Climate Action Network Europe':                    'ONG',
    'ClientEarth AISBL':                                'ONG',
    'Bellona Europa':                                   'ONG',
    'Friends of the Earth Europe':                      'ONG',
    'Transport and Environment (European Federation for Transport and Environment)': 'ONG',
    'Transport and Environment':                        'ONG',
    'ECODES':                                           'ONG',
    'Mighty Earth':                                     'ONG',

    # ── MEDIO AMBIENTE ─────────────────────────────────────────────────────
    # Organizaciones o empresas cuyo objeto principal es medioambiental (no ONGs)
    'Cleantech for Europe':                             'Medio ambiente',
    'ECOEMBALAJES ESPAÑA, S.A.':                        'Medio ambiente',
    'Ecoembalajes España S.A.':                         'Medio ambiente',
    'Ecoembalajes España, S.A.':                        'Medio ambiente',
    'ASSEDEL':                                          'Medio ambiente',

    # ── INSTITUCIÓN PÚBLICA ────────────────────────────────────────────────
    'Gobierno Vasco':                                   'Institución pública',
    'Eurocities':                                       'Institución pública',
    'Banco de España':                                  'Institución pública',
    'UNODC':                                            'Institución pública',
    'Kurdish Democratic Party':                         'Institución pública',
    'Party of European Socialists':                     'Institución pública',
    'Generalitat de Catalunya':                         'Institución pública',
    'Colegio de Registradores de España':               'Institución pública',
    'City of London Corporation':                       'Institución pública',
    'Sumar':                                            'Institución pública',
    'Partito Democratico':                              'Institución pública',
    'Sinn Féin':                                        'Institución pública',

    # ── INSTITUCIÓN UE ─────────────────────────────────────────────────────
    'Comité Económico y Social Europeo':                'Institución UE',

    # ── THINK TANK ─────────────────────────────────────────────────────────
    'European Endowment for Democracy':                 'Think tank',
    'Friedrich-Ebert-Stiftung':                         'Think tank',
    'Fundación Alternativas':                           'Think tank',

    # ── EDUCACIÓN ──────────────────────────────────────────────────────────
    'Ramón Pacheco Pardo':                              'Educación',
    'TECNALIA Research & Innovation':                   'Educación',

    # ── MEDIOS ─────────────────────────────────────────────────────────────
    'Association of Commercial Television and Video on Demand Services in Europe (ACT)': 'Medios',
    'Association of Commercial Television and Video on Demand Services in Europe':       'Medios',
    'EUROCINEMA':                                       'Medios',
    'Motion Picture Association EMEA':                  'Medios',
    'European Producers Club':                          'Medios',
    'PROMUSICAE':                                       'Medios',
    'EBU-UER (European Broadcasting Union)':            'Medios',
    'European Federation of Journalists':               'Medios',
    'European Grouping of Societies of Authors and Composers GESAC': 'Medios',
    'European Grouping of Societies of Authors and Composers':       'Medios',
    'GESAC - European Grouping of Societies of Authors and Composers':'Medios',
    'News Media Europe':                                'Medios',
    'Financial Times':                                  'Medios',

    # ── COSMÉTICOS Y PERFUMERÍA ────────────────────────────────────────────
    'Cosmetics Europe':                                 'Cosméticos y perfumería',
    'Asociación Nacional de Perfumería y Cosmética':    'Cosméticos y perfumería',
    "L'Oréal":                                          'Cosméticos y perfumería',

    # ── DISTRIBUCIÓN ───────────────────────────────────────────────────────
    'Mercadona SA':                                     'Distribución',
    'ANGED':                                            'Distribución',
    'ANGED - Asociación Nacional Grandes de Empresas de Distribución': 'Distribución',
    'Asociación Nacional de Grandes Empresas de Distribución': 'Distribución',
    'ASEDAS':                                           'Distribución',
    'Asedas':                                           'Distribución',
    'Asociación Española de Distribuidores, Autoservicios y Supermercados': 'Distribución',

    # ── ORGANIZACIONES EMPRESARIALES ───────────────────────────────────────
    'COPA-COGECA':                                      'Organizaciones empresariales',
    'Copa-Cogeca':                                      'Organizaciones empresariales',
    'COPA COGECA':                                      'Organizaciones empresariales',
    'COPA':                                             'Organizaciones empresariales',
    'Consejo General de Colegios de Gestores Administrativos de España': 'Organizaciones empresariales',
    'Konfekoop':                                        'Organizaciones empresariales',

    # ── SINDICATOS ─────────────────────────────────────────────────────────
    'UPA':                                              'Sindicatos',

    # ── DEPORTE ────────────────────────────────────────────────────────────
    'LALIGA':                                           'Deporte',
}


def actor_matches_row(actor, actores_str):
    """True si el actor aparece en el campo actores (separado por |)."""
    parts = [p.strip() for p in actores_str.split('|')]
    return actor in parts


def apply_corrections(conn, dry_run=False):
    cur = conn.cursor()
    total_updated = 0
    detail_rows = []

    for actor, correct_sector in CORRECTIONS.items():
        # Buscar filas donde aparece este actor con sector incorrecto
        cur.execute(
            """
            SELECT id, actores, sector FROM reuniones
            WHERE sector != ?
              AND (
                    actores = ?
                 OR actores LIKE ?
                 OR actores LIKE ?
                 OR actores LIKE ?
              )
            """,
            (correct_sector, actor,
             actor + '|%', '%|' + actor, '%|' + actor + '|%')
        )
        rows = cur.fetchall()

        # Filtrar en Python para evitar falsos positivos en LIKE
        to_fix = [(rid, act, sec) for rid, act, sec in rows
                  if actor_matches_row(actor, act)]

        if to_fix:
            for rid, act, old_sector in to_fix:
                detail_rows.append((actor, old_sector, correct_sector, rid))
            if not dry_run:
                ids = [r[0] for r in to_fix]
                cur.executemany(
                    "UPDATE reuniones SET sector = ? WHERE id = ?",
                    [(correct_sector, rid) for rid in ids]
                )
            total_updated += len(to_fix)

    if not dry_run:
        conn.commit()

    return total_updated, detail_rows


def main():
    parser = argparse.ArgumentParser(description="Corrige sectores incorrectos por actor")
    parser.add_argument('--db',      default=DEFAULT_DB)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)

    log('=' * 65)
    log('CORRECCIÓN DE SECTORES POR ACTOR')
    log('=' * 65)
    if args.dry_run:
        log('  Modo --dry-run: no se escribirá nada')

    total, detail = apply_corrections(conn, dry_run=args.dry_run)

    # Mostrar detalle
    by_actor = {}
    for actor, old, new, rid in detail:
        key = (actor, old, new)
        by_actor.setdefault(key, 0)
        by_actor[key] += 1

    for (actor, old, new), n in sorted(by_actor.items(), key=lambda x: -x[1]):
        print(f"  {n:>4}  {old:<30} -> {new:<30}  [{actor[:40]}]")

    log('=' * 65)
    if args.dry_run:
        log(f'  Se modificarían {total} registros')
    else:
        log(f'  ✅ {total} registros corregidos')
    log('=' * 65)

    # Distribución final de sectores
    log('\nDistribución final de sectores:')
    rows = conn.execute(
        "SELECT sector, COUNT(*) n FROM reuniones "
        "WHERE sector IS NOT NULL AND sector != '' "
        "GROUP BY sector ORDER BY n DESC"
    ).fetchall()
    for sector, n in rows:
        print(f"  {n:>5}  {sector}")

    conn.close()


if __name__ == '__main__':
    main()
