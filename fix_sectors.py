"""
Comprehensive sector correction script for MEP meetings database.
Run from the EU/ directory: python fix_sectors.py
"""
import sqlite3

conn = sqlite3.connect('meps_es_reuniones.db')
cur = conn.cursor()

# Read sector strings exactly as stored in DB (avoids encoding issues in source)
cur.execute("SELECT DISTINCT sector FROM reuniones WHERE sector IS NOT NULL ORDER BY sector")
sm = {row[0]: row[0] for row in cur.fetchall()}

def get_sector(substr):
    for k in sm:
        if substr in k:
            return k
    raise KeyError(substr)

AUTO      = get_sector('utomoc')     # Automocion
TRANS     = sm['Transporte']
IND       = sm['Industria']
ONG       = sm['ONG']
MEDA      = sm['Medio ambiente']
INST_PUB  = get_sector('blica')      # Institucion publica
INST_UE   = get_sector('n UE')       # Institucion UE
CONS      = get_sector('nsultor')    # Consultoria
TEC       = get_sector('ecnolog')    # Tecnologia
SALUD     = sm['Salud']
AGRI      = sm['Agricultura']
SIND      = sm['Sindicatos']
DEFENSA   = sm['Defensa']
MEDIOS    = sm['Medios']
ENERGIA   = get_sector('nerg')       # Energia
BANCA     = sm['Banca']
ORG_EMP   = sm['Organizaciones empresariales']
DIST      = get_sector('stribuc')    # Distribucion
EDUC      = get_sector('ducac')      # Educacion
THINK     = sm['Think tank']
TUR       = get_sector('urism')      # Turismo
OTROS     = sm['Otros']
COS       = get_sector('rfum')       # Cosmeticos y perfumeria

print("Sector constants loaded OK")
print("  AUTO:", repr(AUTO))
print("  TRANS:", repr(TRANS))
print("  CONS:", repr(CONS))
print("  INST_PUB:", repr(INST_PUB))

changes_log = []
total_changes = 0

def fix(actor_pattern, old_sector, new_sector):
    """Update sector for all rows where actor_pattern is LIKE-matched in actores."""
    global total_changes
    cur.execute(
        "UPDATE reuniones SET sector = ? WHERE actores LIKE ? AND sector = ?",
        (new_sector, '%' + actor_pattern + '%', old_sector)
    )
    count = cur.rowcount
    if count > 0:
        changes_log.append("[%2d] %-58s  %s -> %s" % (count, actor_pattern[:58], old_sector, new_sector))
        total_changes += count
    return count

def fix_exact(actor_exact, old_sector, new_sector):
    """Update sector only when actor appears as sole entry or in pipe-separated list."""
    global total_changes
    cur.execute(
        "UPDATE reuniones SET sector = ? WHERE sector = ? AND ("
        "actores = ? OR actores LIKE ? OR actores LIKE ? OR actores LIKE ?)",
        (new_sector, old_sector, actor_exact,
         actor_exact + '|%', '%|' + actor_exact, '%|' + actor_exact + '|%')
    )
    count = cur.rowcount
    if count > 0:
        changes_log.append("[%2d] %-58s  %s -> %s  [EXACT]" % (count, actor_exact[:58], old_sector, new_sector))
        total_changes += count
    return count

print()
print("Applying fixes...")
print("=" * 100)

# ============================================================
# SECTION 1: AUTOMOCION
# Car/truck manufacturers, auto suppliers, EV ecosystem
# wrongly classified as Transporte or Industria
# ============================================================

# Car manufacturers
fix('Mazda Motor Logistics Europe N.V.', TRANS, AUTO)
fix('Nissan Automotive Europe', TRANS, AUTO)
fix('Tesla Motors Netherlands B.V.', TRANS, AUTO)
fix('Renault Group', TRANS, AUTO)
fix('Lucid Motors', TRANS, AUTO)
fix('HORSE Powertrain', TRANS, AUTO)
fix_exact('Ford', TRANS, AUTO)  # Only standalone "Ford", not "Ford Motor Company"

# Automotive suppliers
fix('Grupo Copo', TRANS, AUTO)        # Spanish auto parts manufacturer
fix('FORVIA', MEDA, AUTO)             # Faurecia+Hella automotive supplier
fix('Michelin', TRANS, AUTO)          # tyre manufacturer

# Automotive associations and industry bodies
fix('Sernauto', TRANS, AUTO)          # Spanish auto parts association
fix('Automotive Regions Alliance', TRANS, AUTO)
fix('German Association of the Automotive Industry', TRANS, AUTO)
fix('ACEA - Association of European Automobile Manufacturers', TRANS, AUTO)
fix('Faconauto', TRANS, AUTO)         # Spanish car dealers association
fix('Faconauto', ORG_EMP, AUTO)
fix('ADPA - European Independent Automotive Data Publishers Association', TRANS, AUTO)
fix('AFCAR - Alliance for the Freedom of Car Repair in Europe', TRANS, AUTO)
fix('Asociaci', TRANS, AUTO)          # catches: Asociacion para la Movilidad Electrica
                                      # AND Asociacion Espanola para el Tratamiento Medioambiental
# NOTE: This broad 'Asociaci' match in TRANS->AUTO is safe because in TRANSPORTE sector,
# the only "Asociaci*" actors are automotive-related (verified above)

# EV and Electromobility
fix('ChargeUp Europe', TRANS, AUTO)
fix('Fastned BV', TRANS, AUTO)
fix('Zunder', TRANS, AUTO)            # Spanish EV charging network
fix('ABB E-mobility AB', TRANS, AUTO)
fix('ABB E-Mobility', TRANS, AUTO)
fix('AEDIVE', TRANS, AUTO)            # Spanish EV association
fix('Grupo EasyCharger SA', TRANS, AUTO)
fix('Einride AB', TRANS, AUTO)        # electric truck technology company
fix('Platform for electromobility', TRANS, AUTO)

# EV battery manufacturers
fix('CATL', IND, AUTO)                # world's largest EV battery manufacturer
fix('Association of European Automotive and Industrial Battery Manufacturers', IND, AUTO)

# End-of-life vehicles association (SIGRAUTO)
fix('Tratamiento Medioambiental de los Veh', MEDA, AUTO)

# ITV (vehicle inspection) association
fix('Entidades Colaboradoras con la Administraci', TRANS, AUTO)

# ============================================================
# SECTION 2: CONFLICT RESOLUTION
# Actors appearing with multiple sectors - fix the minority/wrong cases
# ============================================================

# Vinces Consulting: 1 Industria -> Consultoria (lobbying consultancy)
fix('Vinces Consulting', IND, CONS)

# European Environmental Bureau: 1 Medio ambiente -> ONG (environmental NGO)
fix('European Environmental Bureau', MEDA, ONG)

# ASAJA: 1 Agricultura -> Org.Emp. (agricultural business association, not a sector meeting)
fix_exact('ASAJA', AGRI, ORG_EMP)

# European Youth Forum: 1 Sindicatos -> ONG
fix('European Youth Forum', SIND, ONG)

# Acumen Public Affairs: 1 Org.Emp. -> Consultoria
fix('Acumen Public Affairs', ORG_EMP, CONS)

# ECOEMBALAJES: 1 Consultoria -> Medio ambiente (packaging waste company)
fix('ECOEMBALAJES', CONS, MEDA)

# Sumar: 1 Sindicatos -> Institucion publica (Spanish political party)
fix_exact('Sumar', SIND, INST_PUB)

# Comite Economico y Social Europeo: 1 Sindicatos -> Institucion UE
fix_exact('Comit\u00e9 Econ\u00f3mico y Social Europeo', SIND, INST_UE)
# Use partial match as fallback
fix('Econ', SIND, INST_UE)  # catches 'Comite Economico y Social Europeo' in Sindicatos

# Asociacion Espanola de Quiropractica: 1 Consultoria -> Salud
fix('uiropr', CONS, SALUD)

# CEPESCA: 1 Org.Emp. -> Agricultura (fishing federation)
fix('CEPESCA', ORG_EMP, AGRI)

# 5Rights Foundation: 1 Tecnologia -> ONG (digital rights NGO)
fix('5Rights Foundation', TEC, ONG)

# UPA: 1 Agricultura -> Sindicatos (farmers union when appearing alone)
fix_exact('UPA', AGRI, SIND)

# ClientEarth AISBL: 1 Medio ambiente -> ONG (environmental law NGO)
fix('ClientEarth AISBL', MEDA, ONG)

# Friends of the Earth Europe: 1 Medio ambiente -> ONG
fix('Friends of the Earth Europe', MEDA, ONG)

# UNODC: 1 ONG -> Institucion publica (UN Office on Drugs and Crime)
fix('UNODC', ONG, INST_PUB)

# Raytheon Technologies: 1 Org.Emp. -> Defensa (defense company)
fix('Raytheon Technologies', ORG_EMP, DEFENSA)

# Friedrich-Ebert-Stiftung: 1 ONG -> Think tank (German political foundation)
fix('Friedrich-Ebert-Stiftung', ONG, THINK)

# European Council on Foreign Relations: 1 ONG -> Think tank
fix('European Council on Foreign Relations', ONG, THINK)

# Hanbury Strategy and Communications: 1 Tecnologia -> Consultoria (PR/lobbying firm)
fix('Hanbury Strategy and Communications', TEC, CONS)

# TECNALIA Research & Innovation: 1 Industria -> Educacion (R&D/research center)
fix('TECNALIA Research', IND, EDUC)

# Airbnb: 1 Otros -> Tecnologia (tech platform)
fix('Airbnb', OTROS, TEC)

# British Chamber of Commerce EU & Belgium: 1 Institucion publica -> Org.Emp.
fix('British Chamber of Commerce EU', INST_PUB, ORG_EMP)

# FTI Consulting Belgium: 1 Institucion publica -> Consultoria (PR/consulting firm)
fix('FTI Consulting Belgium', INST_PUB, CONS)

# Representacion Permanente de Polonia: 1 Institucion UE -> Institucion publica
fix('Permanente de Polonia', INST_UE, INST_PUB)

# Penta Group: 1 Institucion publica -> Consultoria (lobbying firm)
fix('Penta Group', INST_PUB, CONS)

# Kreab: 1 Institucion publica -> Consultoria (PR/lobbying firm)
fix('Kreab', INST_PUB, CONS)

# Smiths Group plc: 1 Institucion publica -> Industria
fix('Smiths Group plc', INST_PUB, IND)

# General Electric Company: 1 Org.Emp. -> Industria (industrial conglomerate)
fix('General Electric Company', ORG_EMP, IND)

# European Public Real Estate Association: 1 Otros -> Org.Emp.
fix('European Public Real Estate Association', OTROS, ORG_EMP)

# Apartur: 1 Otros -> Turismo (tourist apartment association)
fix('Apartur', OTROS, TUR)

# NAFO: 1 Consultoria -> Org.Emp. (fisheries management organization)
fix('NAFO', CONS, ORG_EMP)

# GreenLight Biosciences: 1 Consultoria -> Agricultura (agri biotech company)
fix('GreenLight Biosciences', CONS, AGRI)

# EPPA SA: 1 Org.Emp. -> Consultoria (lobbying firm)
fix('EPPA SA', ORG_EMP, CONS)

# UPLA: 1 Otros -> ONG
fix('UPLA', OTROS, ONG)

# Biocontrol Coalition: 1 Org.Emp. -> Agricultura
fix('Biocontrol Coalition', ORG_EMP, AGRI)

# Financial Times: 1 Energia -> Medios (newspaper, was in energy meeting)
fix('Financial Times', ENERGIA, MEDIOS)

# Confederacion Espanola de Transporte de Mercancias CETM: 1 Org.Emp. -> Transporte
fix('CETM', ORG_EMP, TRANS)

# Asociacion Nacional de Grandes Empresas de Distribucion: 1 Org.Emp. -> Distribucion
fix('Grandes Empresas de Distribuci', ORG_EMP, DIST)

# Asociacion Nacional de Perfumeria y Cosmetica: 1 Industria -> Cosmeticos
fix('Perfumer', IND, COS)

# Telefonica: 1 Medios -> Tecnologia (telecom company, not media)
fix('elef', MEDIOS, TEC)

# ============================================================
# SECTION 3: ENTITY RECLASSIFICATIONS
# ============================================================

# Malalai Joya - Afghan politician/activist, wrongly classified as Institucion UE
fix('Malalai Joya', INST_UE, ONG)

# Cabildo de El Hierro - local government body, wrongly as Think tank
fix('Cabildo de El Hierro', THINK, INST_PUB)

# BP: 1 Institucion publica -> Energia (energy company in BritCham meeting)
fix_exact('BP', INST_PUB, ENERGIA)

# ACCEM: 1 Sindicatos -> ONG (refugee/migration NGO)
fix('ACCEM', SIND, ONG)

# United Nations (main org): 1 ONG -> Institucion publica
# Row 1131: INTAL|UNRWA|United Nations - this should be ONG because UNRWA context
# But "United Nations" as an entity is intergovernmental -> Institucion publica
# Actually given the humanitarian context (UNRWA meeting), keeping as-is is safer
# Only fix standalone "United Nations" cases
fix_exact('United Nations', ONG, INST_PUB)

# ============================================================
# COMMIT
# ============================================================

conn.commit()

print()
print("CHANGES APPLIED:")
print("-" * 100)
for log in changes_log:
    print(" ", log)
print()
print("Total rows modified:", total_changes)
conn.close()
print()
print("Done. Database committed.")
