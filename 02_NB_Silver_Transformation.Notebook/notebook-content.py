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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 3 : TRANSFORMATION SILVER ──────────────────────────
to_process = ["DAILY", "WEEKLY", "MONTHLY", "ANNUAL"] if freq == "ALL" else [freq]

for f in to_process:
    if f == "DAILY":
        process_to_silver("bronze_cis_cip", "silver_cis_cip",
                          select_cols=["CIS", "CIP7", "CIP13", "LIBELLE_PRESENTATION",
                                       "PRIX_MEDICAMENT", "TAUX_REMBOURSEMENT"])
    elif f == "WEEKLY":
        process_to_silver("bronze_cis_cip_dispo_spec", "silver_dispo_spec",
                          select_cols=["CIS", "CIP7", "CODE_STATUT", "LIBELLE_STATUT",
                                       "DATE_DEBUT", "DATE_MISE_A_JOUR", "DATE_REMISE_DISPO"])
    elif f == "MONTHLY":
        process_to_silver("bronze_cis_bdpm", "silver_cis_bdpm",
                          select_cols=["CIS", "TITULAIRE", "ETAT_COMMERCIALISATION", "DENOMINATION"])
        # ← key_cols="CIS" force le dédoublonnage correct
        # ← apply_healthtek_scoring() dans apply_healthtek_quality() calcule
        #    automatiquement ValeurSmr_Score et ValeurAsmr_Score
        process_to_silver("bronze_cis_has_smr", "silver_has_smr",
                          select_cols=["CIS", "VALEUR_SMR", "LIBELLE_SMR", "DATE_AVIS"],
                          key_cols=["CIS"])
        process_to_silver("bronze_cis_has_asmr", "silver_has_asmr",
                          select_cols=["CIS", "VALEUR_ASMR", "LIBELLE_ASMR", "DATE_AVIS"],
                          key_cols=["CIS"])
        process_to_silver("bronze_cis_gener", "silver_cis_gener",
                          select_cols=["CIS", "ID_GROUPE", "LIBELLE_GROUPE", "TYPE_GENERIQUE"])
        process_to_silver("bronze_cis_compo", "silver_cis_compo",
                          select_cols=["CIS", "NOM_SUBSTANCE"])
        process_to_silver("bronze_cis_cpd", "silver_cis_cpd")
    elif f == "ANNUAL":
        process_to_silver("bronze_open_medic", "silver_open_medic",
                          select_cols=["CIP13", "BOITES", "REM", "BSE",
                                       "ATC5", "AGE", "SEXE", "BEN_REG",
                                       "TOP_GEN", "GEN_NUM", "PSP_SPE"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Elle liste toutes les tables disponibles et leurs chemins réels
# Méthode 1 : via le catalogue Spark
print("=== TABLES DANS LE CATALOGUE SPARK ===")
spark.sql("SHOW TABLES").show(50, truncate=False)

# Méthode 2 : via mssparkutils (liste les dossiers physiques dans /Tables)
print("\n=== DOSSIERS PHYSIQUES DANS /Tables ===")
try:
    files = mssparkutils.fs.ls(
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
        f"/{LAKEHOUSE_ID}/Tables"
    )
    for f in files:
        print(f"  {f.name}  →  {f.path}")
except Exception as e:
    print(f"Erreur listing : {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************


# CELLULE 4 : ASSEMBLAGE MASTER ───────────────────────────────
try:
    fact_cis = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_cis_bdpm")
        .select("CIS", "Titulaire", "EtatCommercialisation")
        .dropDuplicates(["CIS"])
    )
    dim_cip = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_cis_cip")
        .select("CIS", "CIP13", "LibellePresentation", "PrixMedicament", "TauxRemboursement")
        .dropDuplicates(["CIP13"])
    )
    dim_dispo = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_dispo_spec")
        .select("CIS", "CIP13", "CodeStatut", "LibelleStatut",
                "DateDebut", "DateMiseJour", "DateRemise")
        .dropDuplicates(["CIS"])
    )

    window_spec = Window.partitionBy("CIS").orderBy(F.col("DateAvis").desc())

    dim_smr = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_has_smr")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter("rn = 1")
        .select("CIS", "ValeurSmr", "LibelleSmr", "ValeurSmr_Score")
    )
    dim_asmr = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_has_asmr")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter("rn = 1")
        .select("CIS", "ValeurAsmr", "LibelleAsmr", "ValeurAsmr_Score")
    )

    if spark.catalog.tableExists("silver_cis_compo"):
        dim_compo = (
            spark.read.format("delta").load(f"{TABLES_ROOT}/silver_cis_compo")
            .groupBy("CIS")
            .agg(F.concat_ws(" + ", F.collect_list("NomSubstance")).alias("CompositionSubstances"))
        )
    else:
        dim_compo = spark.createDataFrame([], "CIS string, CompositionSubstances string")

    dim_gener = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_cis_gener")
        .select("CIS", "IdGroupe", "LibelleGroupe", "TypeGenerique")
        .dropDuplicates(["CIS"])
    )

    fact_om = (
        spark.read.format("delta").load(f"{TABLES_ROOT}/silver_open_medic")
        .select("CIP13", "Boites", "Rem", "Bse", "ATC5", "ATC1",
                "Age_Label", "Age_Sort_Order", "Sexe_Label",
                "BenReg", "TopGen", "GenNum", "PspSpe")
    )

    master = (
        fact_om
        .join(dim_cip,   "CIP13", "left")
        .join(fact_cis,  "CIS",   "left")
        .join(dim_dispo, "CIS", "left")
        .join(dim_smr,   "CIS",   "left")
        .join(dim_asmr,  "CIS",   "left")
        .join(dim_gener, "CIS",   "left")
        .join(dim_compo, "CIS",   "left")
    )

    master.write.format("delta") \
        .mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable("silver_health_analytics_master")

    logger.info("=== Master Model optimisé créé avec succès ===")
    logger.info(f"=== Lignes : {master.count():,} | Colonnes : {len(master.columns)} ===")

except Exception as e:
    logger.error(f"=== ERREUR ASSEMBLAGE : {str(e)} ===")
    raise   # ← on remonte l'erreur pour la voir dans les logs Fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
