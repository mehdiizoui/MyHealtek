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


# ═══════════════════════════════════════════════════════════════
# NOTEBOOK 00_NB_Setup_Resources
# ═══════════════════════════════════════════════════════════════

# CELLULE 1 ───────────────────────────────────────────────────
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('healthtek')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 2 ───────────────────────────────────────────────────
WORKSPACE_ID   = "7d0be975-834c-4a8b-94e5-285d539839c6"
LAKEHOUSE_ID   = "160b09ed-0f10-429d-a4c2-b61aaa24b00e"
LAKEHOUSE_NAME = "LH_Pharma"

ABFS_ROOT   = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_ID}/Files"
)
TABLES_ROOT = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_ID}/Tables/dbo"
)

CONFIG = {
    "BASE_PATH"       : f"{ABFS_ROOT}/bdpm/",
    "OPENMEDIC_URL"   : f"{ABFS_ROOT}/openmedic/OPEN_MEDIC_2024.CSV",
    "DEFAULT_ENCODING": "WINDOWS-1252",
    "AUDIT_COLUMNS"   : {
        "TIMESTAMP": "IngestionTimestamp",
        "SOURCE"   : "SourceFile"
    }
}

logger.info(f"=== CONFIG chargée – Lakehouse : {LAKEHOUSE_NAME} ===")
logger.info(f"=== ABFS Root   : {ABFS_ROOT} ===")
logger.info(f"=== TABLES Root : {TABLES_ROOT} ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 3 ───────────────────────────────────────────────────
schema_CIS_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("DENOMINATION", StringType(), True),
    StructField("FORME_PHARMACEUTIQUE", StringType(), True),
    StructField("VOIE_ADMINISTRATION", StringType(), True),
    StructField("STATUT_ADMINISTRATIF", StringType(), True),
    StructField("TYPE_PROCEDURE", StringType(), True),
    StructField("ETAT_COMMERCIALISATION", StringType(), True),
    StructField("DATE_AMM", StringType(), True),
    StructField("STATUT_BDM", StringType(), True),
    StructField("NUM_AUTORISATION_EUROPEENNE", StringType(), True),
    StructField("TITULAIRE", StringType(), True),
    StructField("SURVEILLANCE_RENFORCEE", StringType(), True)
])

schema_CIS_CIP_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("CIP7", StringType(), True),
    StructField("LIBELLE_PRESENTATION", StringType(), True),
    StructField("STATUT_ADMINISTRATIF", StringType(), True),
    StructField("ETAT_COMMERCIALISATION", StringType(), True),
    StructField("DATE_DECLARATION", StringType(), True),
    StructField("CIP13", StringType(), True),
    StructField("AGREMENT_COLLECTIVITES", StringType(), True),
    StructField("TAUX_REMBOURSEMENT", StringType(), True),
    StructField("PRIX_MEDICAMENT", StringType(), True),
    StructField("PRIX_PUBLIC_FRANCE", StringType(), True),
    StructField("INDICATIONS_REMBOURSEMENT", StringType(), True)
])

schema_CIS_CIP_DISPO_SPEC = StructType([
    StructField("CIS", StringType(), True),
    StructField("CIP7", StringType(), True),
    StructField("CODE_STATUT", StringType(), True),
    StructField("LIBELLE_STATUT", StringType(), True),
    StructField("DATE_DEBUT", StringType(), True),
    StructField("DATE_MISE_A_JOUR", StringType(), True),
    StructField("DATE_REMISE_DISPO", StringType(), True),
    StructField("LIEN_ANSM", StringType(), True)
])

schema_OPEN_MEDIC = StructType([
    StructField("ATC1", StringType(), True),
    StructField("L_ATC1", StringType(), True),
    StructField("ATC2", StringType(), True),
    StructField("L_ATC2", StringType(), True),
    StructField("ATC3", StringType(), True),
    StructField("L_ATC3", StringType(), True),
    StructField("ATC4", StringType(), True),
    StructField("L_ATC4", StringType(), True),
    StructField("ATC5", StringType(), True),
    StructField("L_ATC5", StringType(), True),
    StructField("CIP13", StringType(), True),
    StructField("L_CIP13", StringType(), True),
    StructField("TOP_GEN", StringType(), True),
    StructField("GEN_NUM", StringType(), True),
    StructField("AGE", StringType(), True),
    StructField("SEXE", StringType(), True),
    StructField("BEN_REG", StringType(), True),
    StructField("PSP_SPE", StringType(), True),
    StructField("BOITES", StringType(), True),
    StructField("REM", StringType(), True),
    StructField("BSE", StringType(), True)
])

schema_CIS_COMPO_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("DESIGNATION_ELEMENT", StringType(), True),
    StructField("CODE_SUBSTANCE", StringType(), True),
    StructField("NOM_SUBSTANCE", StringType(), True),
    StructField("DOSAGE_SUBSTANCE", StringType(), True),
    StructField("UNITE_DOSAGE", StringType(), True),
    StructField("NATURE_COMPOSANT", StringType(), True),
    StructField("NUMERO_LIAISON", StringType(), True)
])

schema_CIS_GENER_bdpm = StructType([
    StructField("ID_GROUPE", StringType(), True),
    StructField("LIBELLE_GROUPE", StringType(), True),
    StructField("CIS", StringType(), True),
    StructField("TYPE_GENERIQUE", StringType(), True),
    StructField("NUMERO_ORDRE", StringType(), True)
])

schema_CIS_HAS_SMR_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("CODE_DOSSIER_HAS", StringType(), True),
    StructField("MOTIF_EVALUATION", StringType(), True),
    StructField("DATE_AVIS", StringType(), True),
    StructField("VALEUR_SMR", StringType(), True),
    StructField("LIBELLE_SMR", StringType(), True)
])

schema_CIS_HAS_ASMR_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("CODE_DOSSIER_HAS", StringType(), True),
    StructField("MOTIF_EVALUATION", StringType(), True),
    StructField("DATE_AVIS", StringType(), True),
    StructField("VALEUR_ASMR", StringType(), True),
    StructField("LIBELLE_ASMR", StringType(), True)
])

schema_CIS_CPD_bdpm = StructType([
    StructField("CIS", StringType(), True),
    StructField("CONDITION_PRESCRIPTION_DELIVRANCE", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 4 ───────────────────────────────────────────────────
def ingest_to_bronze(file_path, schema, table_name, encoding=None, separator="\t", has_header="false"):
    encoding = encoding or CONFIG["DEFAULT_ENCODING"]
    logger.info(f"=== Ingestion Bronze : {table_name} ===")
    try:
        df = (spark.read
            .option("sep", separator)
            .option("encoding", encoding)
            .option("header", has_header)
            .schema(schema)
            .csv(file_path))
        df = df.withColumn(CONFIG["AUDIT_COLUMNS"]["TIMESTAMP"], F.current_timestamp())
        df = df.withColumn(CONFIG["AUDIT_COLUMNS"]["SOURCE"], F.lit(file_path))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    except Exception as e:
        logger.error(f"=== ERREUR INGESTION {table_name} : {str(e)} ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 5 ───────────────────────────────────────────────────
def standardize_columns(df):
    col_map = {}
    for c in df.columns:
        pascal = "".join([w.capitalize() for w in c.split('_')])
        final = pascal.replace("Atc", "ATC").replace("Cip", "CIP").replace("Cis", "CIS")
        if final == "DateMiseAJour": final = "DateMiseJour"
        if final == "DateRemiseDispo": final = "DateRemise"
        col_map[c] = final
    try:
        return df.withColumnsRenamed(col_map)
    except:
        for old_c, new_c in col_map.items():
            df = df.withColumnRenamed(old_c, new_c)
        return df

def handle_technical_normalization(df):
    text_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in text_cols:
        df = df.withColumn(c, F.regexp_replace(F.col(c), "[\u2018\u2019\u201A\u201B]", "'"))
        df = df.withColumn(c, F.regexp_replace(F.col(c), "[\u00A0\u2007\u202F]", " "))
    return df

def apply_default_labels(df):
    replacements = {"StatutBdm": "Non Renseigné", "CodeStatut": "Disponible", "Titulaire": "Inconnu", "LibelleStatut": "Inconnu"}
    for col_name, val in replacements.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.when((F.col(col_name).isNull()) | (F.col(col_name).isin("", "Null", " ")), val).otherwise(F.col(col_name)))
    return df

def apply_healthtek_aesthetic(df, skip_formatting=False):
    if skip_formatting: return df
    text_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    sentence_case_cols = ["LibelleAsmr", "LibelleSmr", "Denomination", "LibelleStatut", "StatutBdm", "LibelleGroupe", "NomSubstance", "EtatCommercialisation"]
    acronym_cols = [c for c in df.columns if any(c.upper().startswith(k) for k in ["CIS", "CIP", "ATC"]) and not c.startswith("L")]
    for c in text_cols:
        if c in df.columns:
            df = df.withColumn(c, F.regexp_replace(F.col(c), "(?i)<br\\s*/?>", " "))
            df = df.withColumn(c, F.regexp_replace(F.col(c), "<[^>]*>", " "))
            df = df.withColumn(c, F.regexp_replace(F.col(c), "(?i)([ldnsjt])\\s*[']\\s*([aeiouyh])", "$1'$2"))
            if c in acronym_cols:
                df = df.withColumn(c, F.upper(F.trim(F.col(c))))
            elif c in sentence_case_cols or len(c) > 15:
                df = df.withColumn(c, F.concat(F.upper(F.substring(F.trim(F.col(c)), 1, 1)), F.lower(F.substring(F.trim(F.col(c)), 2, 10000))))
            else:
                df = df.withColumn(c, F.initcap(F.lower(F.trim(F.col(c)))))
    return df

def apply_healthtek_scoring(df):
    if "ValeurSmr" in df.columns:
        df = df.withColumn("ValeurSmr_Score", F.when(F.col("ValeurSmr").contains("Important"), 1)
                                               .when(F.col("ValeurSmr").contains("Modér"), 2)
                                               .when(F.col("ValeurSmr").contains("Faible"), 3)
                                               .when(F.col("ValeurSmr").contains("Insuffisant"), 4).otherwise(0).cast("integer"))
    if "ValeurAsmr" in df.columns:
        df = df.withColumn("TEMP_EXT", F.regexp_extract(F.upper(F.col("ValeurAsmr")), r"\b(IV|III|II|V|I)\b", 1))
        df = df.withColumn("ValeurAsmr_Score", F.when(F.col("TEMP_EXT") == "V", 5)
                                                .when(F.col("TEMP_EXT") == "IV", 4)
                                                .when(F.col("TEMP_EXT") == "III", 3)
                                                .when(F.col("TEMP_EXT") == "II", 2)
                                                .when(F.col("TEMP_EXT") == "I", 1).otherwise(0).cast("integer")).drop("TEMP_EXT")
    return df

def convert_healthtek_types(df):
    int_cols = ["Boites", "Age", "Sexe", "BenReg", "PspSpe", "TopGen", "GenNum", "IdGroupe", "IdentifiantGroupe"]
    for c in int_cols:
        if c in df.columns: df = df.withColumn(c, F.coalesce(F.regexp_replace(F.col(c), ",", ".").cast("integer"), F.lit(0)))
    double_cols = ["PrixMedicament", "PrixPublicFrance", "Rem", "Bse", "Prix"]
    for c in double_cols:
        if c in df.columns: df = df.withColumn(c, F.coalesce(F.regexp_replace(F.col(c), ",", ".").cast("double"), F.lit(0.0)))
    if "TauxRemboursement" in df.columns:
        expr = F.regexp_replace(F.regexp_replace(F.col("TauxRemboursement"), "%", ""), ",", ".").cast("double")
        df = df.withColumn("TauxRemboursement", F.coalesce(expr / F.when(F.col("TauxRemboursement").contains("%"), 100.0).otherwise(1.0), F.lit(0.0)))
    date_cols = [c for c in df.columns if any(k in c for k in ["Date", "Avis", "Debut", "MiseJour", "Remise"])]
    for c in date_cols: df = df.withColumn(c, F.coalesce(F.to_date(F.col(c), "dd/MM/yyyy"), F.to_date(F.col(c), "yyyy-MM-dd"), F.to_date(F.col(c), "yyyyMMdd")))
    return df

def apply_healthtek_padding(df):
    if "CIS" in df.columns:   df = df.withColumn("CIS",   F.lpad(F.trim(F.col("CIS")).cast("string"),   8, "0"))
    if "CIP13" in df.columns: df = df.withColumn("CIP13", F.lpad(F.trim(F.col("CIP13")).cast("string"), 13, "0"))
    if "CIP7" in df.columns:  df = df.withColumn("CIP7",  F.lpad(F.trim(F.col("CIP7")).cast("string"),   7, "0"))
    if "ATC5" in df.columns:  df = df.withColumn("ATC5",  F.upper(F.trim(F.col("ATC5"))))
    return df.withColumn("Annee_Volume_Ref", F.lit(2024))

def apply_healthtek_quality(df, skip_formatting=False, drop_audit=True):
    df = standardize_columns(df)
    df = handle_technical_normalization(df)
    df = apply_default_labels(df)
    df = apply_healthtek_aesthetic(df, skip_formatting)
    df = apply_healthtek_scoring(df)
    df = convert_healthtek_types(df)
    df = apply_healthtek_padding(df)
    if drop_audit:
        audit_cols = ["IngestionTimestamp", "SourceFile", "time", "source"]
        df = df.drop(*[c for c in audit_cols if c in df.columns])
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 6 ───────────────────────────────────────────────────
def transform_openmedic_demographics(df):
    df = df.withColumn("Age_Label",
        F.when(F.col("Age").isin(0, 10, 19), "0-19")
         .when(F.col("Age").isin(20, 40, 59), "20-59")
         .when(F.col("Age").isin(60, 80, 99), "60-")
         .otherwise("Âge Inconnu"))
    df = df.withColumn("Age_Sort_Order",
        F.when(F.col("Age_Label") == "0-19",  1)
         .when(F.col("Age_Label") == "20-59", 2)
         .when(F.col("Age_Label") == "60-",   3).otherwise(9))
    df = df.withColumn("Sexe_Label",
        F.when(F.col("Sexe") == 1, "Homme")
         .when(F.col("Sexe") == 2, "Femme")
         .otherwise("Inconnu"))
    if "ATC5" in df.columns:
        df = df.withColumn("ATC1", F.substring(F.col("ATC5"), 1, 1))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# CELLULE 7 ───────────────────────────────────────────────────
def process_to_silver(source_table, target_table, key_cols=None, select_cols=None):
    if spark.catalog.tableExists(source_table):
        try:
            df = spark.read.table(source_table)
            if select_cols:
                df = df.select(*select_cols)
            df_clean = apply_healthtek_quality(df)
            if "dispo_spec" in source_table.lower():
                if spark.catalog.tableExists("silver_cis_cip"):
                    mapping_cip = spark.read.table("silver_cis_cip").select("CIS", "CIP7", "CIP13")
                    df_clean = df_clean.join(mapping_cip, ["CIS", "CIP7"], "left")
            if "open_medic" in source_table.lower():
                df_clean = transform_openmedic_demographics(df_clean)
            if not key_cols:
                key_cols = ["CIP13"] if "CIP13" in df_clean.columns else ["CIS"] if "CIS" in df_clean.columns else None
            if key_cols:
                df_clean = df_clean.dropDuplicates(key_cols)
            df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            logger.info(f"=== Table Silver créée : {target_table} ===")
        except Exception as e:
            logger.error(f"=== ERREUR {target_table} : {str(e)} ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELLULE 8 ───────────────────────────────────────────────────
def compute_silver_metrics(df):
    win_atc = Window.partitionBy("ATC5")
    win_cip = Window.partitionBy("CIP13")
    if all(c in df.columns for c in ["Boites", "Rem"]):
        df = df.withColumn("CoutMoyenBoite", F.round(F.col("Rem") / F.col("Boites"), 2))
        df = df.withColumn("DeltaPrixGroupe", F.round(F.max("CoutMoyenBoite").over(win_atc) - F.min("CoutMoyenBoite").over(win_atc), 2))
        df = df.withColumn("CvConsommation",  F.round((F.stddev("Boites").over(win_cip) / F.avg("Boites").over(win_cip)) * 100, 2))
    return df

logger.info("=== Ressources chargées avec succès ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_to_silver(source_table, target_table, key_cols=None, select_cols=None):
    """
    PIPELINE DE PASSAGE BRONZE -> SILVER.
    Optimisée par la sélection précoce de colonnes (select_cols).
    """
    if spark.catalog.tableExists(source_table):
        try:
            df = spark.read.table(source_table)
            
            # OPTIMISATION : Sélection précoce (Pruning) pour alléger les traitements
            if select_cols:
                df = df.select(*select_cols)
                logger.info(f"=== Optimisation : Pruning de {len(select_cols)} colonnes pour {target_table} ===")

            df_clean = apply_healthtek_quality(df)
            
            # ENRICHISSEMENT : Ajout CIP13 pour dispo_spec via lookup sur cis_cip
            if "dispo_spec" in source_table.lower():
                if spark.catalog.tableExists("silver_cis_cip"):
                    mapping_cip = spark.read.table("silver_cis_cip").select("CIS", "CIP7", "CIP13")
                    df_clean = df_clean.join(mapping_cip, ["CIS", "CIP7"], "left")
                    logger.info(f"=== Enrichment : Ajout CIP13 à {target_table} ===")
            
            if "open_medic" in source_table.lower(): df_clean = transform_openmedic_demographics(df_clean)
            
            # DÉDOUBLONNAGE : Assure l'unicité du grain (CIP13 ou CIS)
            if not key_cols:
                key_cols = ["CIP13"] if "CIP13" in df_clean.columns else ["CIS"] if "CIS" in df_clean.columns else None
            if key_cols: df_clean = df_clean.dropDuplicates(key_cols)
            
            df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)
            logger.info(f"===  Table Silver créee : {target_table} ===")
        except Exception as e:
            logger.error(f"===  ERREUR {target_table} : {str(e)} ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
