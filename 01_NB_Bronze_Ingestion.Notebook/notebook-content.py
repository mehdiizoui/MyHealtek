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

# Fabric parameters
try:
    freq = notebookParams["FREQUENCE"]
except:
    freq = "ALL"   # valeur par défaut

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === ========================================================================= ===
# ===                       EXÉCUTION DE L'INGESTION BRONZE                    ===
# === ========================================================================= ===

base_path = CONFIG["BASE_PATH"]
to_do = ["DAILY", "WEEKLY", "MONTHLY", "ANNUAL"] if freq == "ALL" else [freq]

logger.info(f"=== Démarrage Ingestion Bronze - Fréquence : {freq} ===")

for f in to_do:
    if f == "DAILY":
        ingest_to_bronze(f"{base_path}CIS_CIP_bdpm*.txt", schema_CIS_CIP_bdpm, "bronze_cis_cip")
    elif f == "WEEKLY":
        ingest_to_bronze(f"{base_path}CIS_CIP_Dispo_Spec*.txt", schema_CIS_CIP_DISPO_SPEC, "bronze_cis_cip_dispo_spec")
    elif f == "MONTHLY":
        ingest_to_bronze(f"{base_path}CIS_bdpm*.txt", schema_CIS_bdpm, "bronze_cis_bdpm")
        ingest_to_bronze(f"{base_path}CIS_HAS_SMR_bdpm*.txt", schema_CIS_HAS_SMR_bdpm, "bronze_cis_has_smr")
        ingest_to_bronze(f"{base_path}CIS_HAS_ASMR_bdpm*.txt", schema_CIS_HAS_ASMR_bdpm, "bronze_cis_has_asmr")
        ingest_to_bronze(f"{base_path}CIS_GENER_bdpm*.txt", schema_CIS_GENER_bdpm, "bronze_cis_gener")
        ingest_to_bronze(f"{base_path}CIS_COMPO_bdpm*.txt", schema_CIS_COMPO_bdpm, "bronze_cis_compo")
        ingest_to_bronze(f"{base_path}CIS_CPD_bdpm*.txt", schema_CIS_CPD_bdpm, "bronze_cis_cpd")
    elif f == "ANNUAL":
        ingest_to_bronze(CONFIG["OPENMEDIC_URL"], schema_OPEN_MEDIC, "bronze_open_medic", 
                         encoding="WINDOWS-1252", separator=";", has_header="true")
logger.info(f"=== Ingestion Bronze terminée avec succès ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
