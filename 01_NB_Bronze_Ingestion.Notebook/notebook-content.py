# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "160b09ed-0f10-429d-a4c2-b61aaa24b00e",
# META       "default_lakehouse_name": "LH_Pharma",
# META       "default_lakehouse_workspace_id": "7d0be975-834c-4a8b-94e5-285d539839c6",
# META       "known_lakehouses": [
# META         {
# META           "id": "160b09ed-0f10-429d-a4c2-b61aaa24b00e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


%run ./00_NB_Setup_Resources

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 2 ───────────────────────────────────────────────────
try:
    freq = notebookParams["FREQUENCE"]
except:
    freq = "ALL"

try:
    year = int(notebookParams["ANNEE"])
except:
    year = 2024

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 3 ───────────────────────────────────────────────────
import requests, zipfile, io
from bs4 import BeautifulSoup

def download_open_medic(year: int) -> list:
    token_url = (
        f"https://open-data-assurance-maladie.ameli.fr/medicaments/download.php"
        f"?Dir_Rep=Open_MEDIC_Base_Complete&Annee={year}"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer"   : "https://open-data-assurance-maladie.ameli.fr/",
    }
    session = requests.Session()
    page = session.get(token_url, headers=headers, timeout=30)
    page.raise_for_status()
    page.encoding = "iso-8859-1"

    soup = BeautifulSoup(page.text, "html.parser")
    download_url = None
    for a in soup.find_all("a", href=True):
        if "download_file.php" in a["href"]:
            href = a["href"]
            base = "https://open-data-assurance-maladie.ameli.fr/medicaments/"
            download_url = href if href.startswith("http") else base + href.lstrip("./")
            break

    if not download_url:
        raise RuntimeError(f"[Open Medic] Lien introuvable pour {year}")

    logger.info(f"[Open Medic] URL résolue : {download_url}")
    resp = session.get(download_url, headers=headers, timeout=180)
    resp.raise_for_status()
    logger.info(f"[Open Medic] ZIP téléchargé : {len(resp.content)/1e6:.1f} Mo")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_files = [n for n in zf.namelist() if n.upper().endswith(".CSV")]
        if not csv_files:
            raise RuntimeError(f"[Open Medic] Aucun CSV dans le ZIP : {zf.namelist()}")
        lines = zf.read(csv_files[0]).decode("iso-8859-1").splitlines()

    logger.info(f"[Open Medic] {len(lines):,} lignes extraites")
    return lines


def ingest_open_medic_to_bronze(year: int):
    lines  = download_open_medic(year)
    header = lines[0]
    rdd    = spark.sparkContext.parallelize(lines[1:])

    df = spark.read.csv(rdd, header=False, sep=";").toDF(*header.split(";"))

    expected_cols = [f.name for f in schema_OPEN_MEDIC.fields]
    for col in expected_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))
    df = df.select(expected_cols)

    df = (df
        .withColumn(CONFIG["AUDIT_COLUMNS"]["TIMESTAMP"], F.current_timestamp())
        .withColumn(CONFIG["AUDIT_COLUMNS"]["SOURCE"],
                    F.lit(f"ameli.fr/Open_MEDIC_Base_Complete/{year}"))
        .withColumn("Annee", F.lit(year))
    )

    df.write.format("delta") \
        .mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("bronze_open_medic")

    logger.info(f"[Open Medic] ✅ bronze_open_medic sauvegardée ({year})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 4 ───────────────────────────────────────────────────
base_path = CONFIG["BASE_PATH"]
to_do = ["DAILY", "WEEKLY", "MONTHLY", "ANNUAL"] if freq == "ALL" else [freq]

logger.info(f"=== Démarrage Ingestion Bronze – Fréquence : {freq} | Année : {year} ===")

for f in to_do:
    if f == "DAILY":
        ingest_to_bronze(f"{base_path}CIS_CIP_bdpm*.txt",          schema_CIS_CIP_bdpm,       "bronze_cis_cip")
    elif f == "WEEKLY":
        ingest_to_bronze(f"{base_path}CIS_CIP_Dispo_Spec*.txt",    schema_CIS_CIP_DISPO_SPEC, "bronze_cis_cip_dispo_spec")
    elif f == "MONTHLY":
        ingest_to_bronze(f"{base_path}CIS_bdpm*.txt",              schema_CIS_bdpm,            "bronze_cis_bdpm")
        ingest_to_bronze(f"{base_path}CIS_HAS_SMR_bdpm*.txt",      schema_CIS_HAS_SMR_bdpm,   "bronze_cis_has_smr")
        ingest_to_bronze(f"{base_path}CIS_HAS_ASMR_bdpm*.txt",     schema_CIS_HAS_ASMR_bdpm,  "bronze_cis_has_asmr")
        ingest_to_bronze(f"{base_path}CIS_GENER_bdpm*.txt",        schema_CIS_GENER_bdpm,      "bronze_cis_gener")
        ingest_to_bronze(f"{base_path}CIS_COMPO_bdpm*.txt",        schema_CIS_COMPO_bdpm,      "bronze_cis_compo")
        ingest_to_bronze(f"{base_path}CIS_CPD_bdpm*.txt",          schema_CIS_CPD_bdpm,        "bronze_cis_cpd")
    elif f == "ANNUAL":
        ingest_open_medic_to_bronze(year)

logger.info("=== Ingestion Bronze terminée avec succès ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
