# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4da0c1b7-9134-40c8-9c73-b8b4df8811aa",
# META       "default_lakehouse_name": "LH_Pharma",
# META       "default_lakehouse_workspace_id": "ddedbaa1-3de9-4b61-b43d-ca13f34152d6",
# META       "known_lakehouses": [
# META         {
# META           "id": "4da0c1b7-9134-40c8-9c73-b8b4df8811aa"
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
from pyspark.sql.window import Window

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

# CELLULE 3 : CHARGEMENT DU MASTER ───────────────────────────
logger.info("=== [Gold] Chargement silver_health_analytics_master ===")

master = (
    spark.read.format("delta")
    .load(f"{TABLES_ROOT}/silver_health_analytics_master")
    .withColumn("CIP13", F.lpad(F.trim(F.col("CIP13")), 13, "0"))
)

logger.info(f"=== [Gold] Master chargé : {master.count():,} lignes ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 4 : DIM_MEDICAMENT ──────────────────────────────────
logger.info("=== [Gold] Construction Dim_Medicament ===")

dim_medicament = (
    master
    .select("CIP13", "CIS", "LibellePresentation", "Titulaire",
            "EtatCommercialisation", "PrixMedicament", "TauxRemboursement",
            "TypeGenerique", "IdGroupe", "LibelleGroupe",
            "CompositionSubstances", "ATC5", "ATC1",
            F.lit(year).alias("Annee_Ref"))
    .dropDuplicates(["CIP13"])
    .withColumn("Est_Generique",
        F.when(F.col("TypeGenerique").isin(1, 4), F.lit("Oui"))
         .when(F.col("TypeGenerique") == 0,       F.lit("Princeps"))
         .otherwise(F.lit("Inconnu")))
)

dim_medicament.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_medicament")

logger.info(f"=== [Gold] ✅ gold_dim_medicament : {dim_medicament.count():,} médicaments ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 5 : DIM_RUPTURE ─────────────────────────────────────
logger.info("=== [Gold] Construction Dim_Rupture ===")

dim_rupture = (
    master
    .select("CIP13", "CIS", "CodeStatut", "LibelleStatut",
            "DateDebut", "DateMiseJour", "DateRemise")
    .filter(F.col("CodeStatut").isNotNull())
    .dropDuplicates(["CIP13", "CodeStatut", "DateDebut"])
    .withColumn("Statut_Libelle_Norm",
        F.when(F.col("CodeStatut") == "1", F.lit("Rupture de stock"))
         .when(F.col("CodeStatut") == "2", F.lit("Tension d'approvisionnement"))
         .when(F.col("CodeStatut") == "3", F.lit("Arrêt de commercialisation"))
         .when(F.col("CodeStatut") == "4", F.lit("Remise à disposition"))
         .otherwise(F.col("LibelleStatut")))
    .withColumn("Est_Rupture_Active",
        F.when(
            F.col("CodeStatut").isin("1", "2") & F.col("DateRemise").isNull(),
            F.lit(True)
        ).otherwise(F.lit(False)))
)

dim_rupture.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_rupture")

logger.info(f"=== [Gold] ✅ gold_dim_rupture : {dim_rupture.count():,} entrées ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 6 : DIM_EVALUATION ──────────────────────────────────
logger.info("=== [Gold] Construction Dim_Evaluation ===")

dim_evaluation = (
    master
    .select("CIS", "CIP13", "ValeurSmr", "LibelleSmr",
            "ValeurAsmr", "LibelleAsmr",
            "ValeurSmr_Score", "ValeurAsmr_Score")
    .dropDuplicates(["CIS"])
    .withColumn("SMR_Categorie",
        F.when(F.col("ValeurSmr_Score") == 1, F.lit("Important"))
         .when(F.col("ValeurSmr_Score") == 2, F.lit("Modéré"))
         .when(F.col("ValeurSmr_Score") == 3, F.lit("Faible"))
         .when(F.col("ValeurSmr_Score") == 4, F.lit("Insuffisant"))
         .otherwise(F.lit("Non évalué")))
)

dim_evaluation.write.format("delta") \
    .mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_evaluation")

logger.info(f"=== [Gold] ✅ gold_dim_evaluation : {dim_evaluation.count():,} spécialités ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
