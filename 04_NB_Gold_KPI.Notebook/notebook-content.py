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

YEAR_PROJ = 2026

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 3 : CHARGEMENT ──────────────────────────────────────
logger.info("=== [KPI] Chargement des tables Silver ===")

master = (
    spark.read.format("delta")
    .load(f"{TABLES_ROOT}/silver_health_analytics_master")
    .withColumn("CIP13", F.lpad(F.trim(F.col("CIP13")), 13, "0"))
)

om    = master.select("CIP13", "CIS", "Boites", "Rem", "Bse",
                       "ATC1", "ATC5", "Age_Label", "Age_Sort_Order",
                       "Sexe_Label", "TypeGenerique", "IdGroupe")
cip   = master.select("CIP13", "CIS", "PrixMedicament", "TauxRemboursement").dropDuplicates(["CIP13"])
dispo = master.select("CIS", "CIP13", "CodeStatut", "LibelleStatut",
                       "DateDebut", "DateMiseJour", "DateRemise").filter(F.col("CodeStatut").isNotNull())
has   = master.select("CIS", "ValeurSmr", "ValeurSmr_Score",
                       "ValeurAsmr", "ValeurAsmr_Score").dropDuplicates(["CIS"])
cis   = master.select("CIS", "Titulaire", "EtatCommercialisation").dropDuplicates(["CIS"])

logger.info("=== [KPI] Tables chargées ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 4 : PÔLE I – ANALYSE ÉCONOMIQUE ─────────────────────
logger.info("=== [KPI] Pôle I – Analyse Économique ===")

kpi_depense_reelle = om.agg(
    F.sum("Rem").cast("decimal(15,2)").alias("Depense_Reelle_2024")
)

kpi_projection = om.join(cip, "CIP13", "left") \
    .withColumn("Cout_2026",
        (F.col("Boites") * F.col("PrixMedicament") * F.col("TauxRemboursement")).cast("decimal(15,4)")) \
    .agg(F.sum("Cout_2026").cast("decimal(15,2)").alias(f"Projection_Budget_{YEAR_PROJ}"))

win_groupe = Window.partitionBy("IdGroupe")
prix_generique = om.join(cip, "CIP13", "left") \
    .filter(F.col("TypeGenerique").isin(1, 4)) \
    .withColumn("Prix_Gen_Min", F.min("PrixMedicament").over(win_groupe)) \
    .select("IdGroupe", "Prix_Gen_Min").dropDuplicates(["IdGroupe"])

kpi_economie_gen = om.join(cip, "CIP13", "left") \
    .filter(F.col("TypeGenerique") == 0) \
    .join(prix_generique, "IdGroupe", "left") \
    .withColumn("Gain_Par_Boite", (F.col("PrixMedicament") - F.col("Prix_Gen_Min")).cast("decimal(10,4)")) \
    .withColumn("Economie_Totale", (F.col("Gain_Par_Boite") * F.col("Boites")).cast("decimal(15,2)")) \
    .agg(F.sum("Economie_Totale").cast("decimal(15,2)").alias("Economie_Generiques_Total"))

kpi_penetration_gen = om.agg(
    (F.sum(F.when(F.col("TypeGenerique").isin(1,4), F.col("Boites")).otherwise(0))
     / F.sum("Boites")).cast("decimal(6,4)").alias("Penetration_Generiques")
)

kpi_cout_atc1 = om.groupBy("ATC1").agg(
    F.sum("Bse").cast("decimal(15,2)").alias("Total_Bse"),
    F.countDistinct("CIS").alias("Nb_Specialites"),
    (F.sum("Bse") / F.countDistinct("CIS")).cast("decimal(12,2)").alias("Cout_Moyen_ATC1")
).orderBy(F.desc("Cout_Moyen_ATC1"))

kpi_depense_reelle.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_depense_reelle")
kpi_projection.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_projection_budget")
kpi_economie_gen.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_economie_generiques")
kpi_penetration_gen.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_penetration_generiques")
kpi_cout_atc1.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_cout_atc1")

logger.info("=== [KPI] ✅ Pôle I sauvegardé ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 5 : PÔLE II – MONITORING SANITAIRE ──────────────────
logger.info("=== [KPI] Pôle II – Monitoring Sanitaire ===")

ruptures_actives = dispo.filter(F.col("CodeStatut").isin("1","2") & F.col("DateRemise").isNull())

smr_important   = has.filter(F.col("ValeurSmr_Score") == 1).select("CIS")
nb_smr1         = smr_important.count()
nb_smr1_rupture = ruptures_actives.join(smr_important, "CIS", "inner").select("CIS").distinct().count()

kpi_taux_rupture = spark.createDataFrame(
    [(nb_smr1_rupture, nb_smr1,
      round(nb_smr1_rupture / nb_smr1, 4) if nb_smr1 > 0 else 0.0)],
    ["CIS_Vitaux_En_Rupture", "CIS_Vitaux_Total", "Taux_Rupture_Critique"]
)

kpi_patients_impactes = om.join(
    ruptures_actives.select("CIS","CIP13").distinct(), ["CIS","CIP13"], "inner"
).agg(
    F.sum("Boites").alias("Boites_En_Rupture"),
    F.countDistinct("CIS").alias("Nb_Medicaments_Concernes")
)

kpi_delai_penurie = dispo.filter(
    F.col("DateRemise").isNotNull() & F.col("DateDebut").isNotNull()
).withColumn("Duree_Jours", F.datediff(F.col("DateRemise"), F.col("DateDebut"))) \
.agg(
    F.avg("Duree_Jours").cast("decimal(8,1)").alias("Delai_Moyen_Penurie_Jours"),
    F.max("Duree_Jours").alias("Delai_Max_Jours"),
    F.min("Duree_Jours").alias("Delai_Min_Jours")
)

kpi_tension_atc = ruptures_actives \
    .join(om.select("CIS","ATC1").dropDuplicates(["CIS"]), "CIS", "left") \
    .groupBy("ATC1").agg(F.countDistinct("CIS").alias("Nb_CIS_En_Rupture")) \
    .orderBy(F.desc("Nb_CIS_En_Rupture"))

portefeuille   = cis.groupBy("Titulaire").agg(F.countDistinct("CIS").alias("Nb_CIS_Portefeuille"))
ruptures_labo  = ruptures_actives.join(cis, "CIS", "left") \
    .groupBy("Titulaire").agg(F.countDistinct("CIS").alias("Nb_CIS_En_Rupture"))
kpi_fiabilite_labo = portefeuille.join(ruptures_labo, "Titulaire", "left") \
    .fillna(0, subset=["Nb_CIS_En_Rupture"]) \
    .withColumn("Taux_Rupture_Labo",
        (F.col("Nb_CIS_En_Rupture") / F.col("Nb_CIS_Portefeuille")).cast("decimal(6,4)")) \
    .orderBy(F.desc("Taux_Rupture_Labo"))

kpi_taux_rupture.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_taux_rupture_critique")
kpi_patients_impactes.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_patients_impactes")
kpi_delai_penurie.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_delai_penurie")
kpi_tension_atc.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_tension_atc")
kpi_fiabilite_labo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_fiabilite_labo")

logger.info("=== [KPI] ✅ Pôle II sauvegardé ===")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 6 : PÔLE III – INNOVATION HAS ──────────────────────
logger.info("=== [KPI] Pôle III – Innovation HAS ===")

om_has      = om.join(has, "CIS", "left")
om_has_prix = om_has.join(cip, "CIP13", "left")

kpi_score_innovation = om_has.filter(F.col("ValeurAsmr_Score").isNotNull()).agg(
    (F.sum(F.col("ValeurAsmr_Score") * F.col("Boites")) / F.sum("Boites"))
    .cast("decimal(6,4)").alias("Score_Innovation_Moyen_Pondere")
)

kpi_acces_innovation = om_has.agg(
    (F.sum(F.when(F.col("ValeurAsmr_Score").isin(1,2,3), F.col("Boites")).otherwise(0))
     / F.sum("Boites")).cast("decimal(6,4)").alias("Taux_Acces_Innovation_ASMR_1_3")
)

kpi_premium_innovation = om_has_prix.agg(
    F.avg(F.when(F.col("ValeurAsmr_Score").isin(1,2,3), F.col("PrixMedicament")))
     .cast("decimal(10,4)").alias("Prix_Moy_ASMR_1_3"),
    F.avg(F.when(F.col("ValeurAsmr_Score").isin(4,5), F.col("PrixMedicament")))
     .cast("decimal(10,4)").alias("Prix_Moy_ASMR_4_5"),
).withColumn("Premium_Innovation",
    (F.col("Prix_Moy_ASMR_1_3") / F.col("Prix_Moy_ASMR_4_5")).cast("decimal(6,4)"))

nb_cis_total = has.select("CIS").distinct().count()
nb_cis_asmr5 = has.filter(F.col("ValeurAsmr_Score") == 5).select("CIS").distinct().count()
kpi_metoo = spark.createDataFrame(
    [(nb_cis_asmr5, nb_cis_total,
      round(nb_cis_asmr5 / nb_cis_total, 4) if nb_cis_total > 0 else 0.0)],
    ["Nb_MeToo_ASMR5", "Nb_CIS_Total", "Part_MeToo"]
)

kpi_efficience = om_has.groupBy("ValeurSmr", "ValeurSmr_Score").agg(
    F.sum("Boites").alias("Total_Boites"),
    F.sum("Rem").cast("decimal(15,2)").alias("Total_Rembourse")
).orderBy("ValeurSmr_Score")

kpi_score_innovation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_score_innovation")
kpi_acces_innovation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_acces_innovation")
kpi_premium_innovation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_premium_innovation")
kpi_metoo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_metoo")
kpi_efficience.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_efficience_therapeutique")

logger.info("=== [KPI] ✅ Pôle III sauvegardé ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 7 : PÔLE IV – VEILLE & USAGE ───────────────────────
logger.info("=== [KPI] Pôle IV – Veille & Usage ===")

kpi_senior = om.agg(
    (F.sum(F.when(F.col("Age_Label") == "60-", F.col("Boites")).otherwise(0))
     / F.sum("Boites")).cast("decimal(6,4)").alias("Dependance_Senior_60plus")
)

kpi_genre = om.agg(
    F.sum(F.when(F.col("Sexe_Label") == "Femme", F.col("Boites")).otherwise(0)).alias("Boites_Femme"),
    F.sum(F.when(F.col("Sexe_Label") == "Homme", F.col("Boites")).otherwise(0)).alias("Boites_Homme"),
).withColumn("Ratio_Genre_F_H",
    (F.col("Boites_Femme") / F.col("Boites_Homme")).cast("decimal(6,4)"))

win_pareto        = Window.orderBy(F.desc("Total_Boites"))
total_boites_all  = om.agg(F.sum("Boites")).first()[0]
kpi_pareto = om.groupBy("ATC5").agg(F.sum("Boites").alias("Total_Boites")) \
    .withColumn("Cumul_Boites", F.sum("Total_Boites").over(win_pareto)) \
    .withColumn("Pct_Cumul", (F.col("Cumul_Boites") / F.lit(total_boites_all)).cast("decimal(6,4)")) \
    .withColumn("Dans_Pareto_80", F.col("Pct_Cumul") <= 0.80) \
    .orderBy(F.desc("Total_Boites"))

cis_dans_om   = om.select("CIS").distinct()
kpi_dynamique = cis.join(cis_dans_om, "CIS", "left_anti") \
    .agg(F.count("CIS").alias("Nb_Nouveaux_Produits_Post_2024"))

kpi_cout_patient = om.agg(
    (F.sum("Bse") / F.sum("Boites")).cast("decimal(10,4)").alias("Cout_Patient_Moyen_Par_Boite")
)

kpi_senior.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_dependance_senior")
kpi_genre.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_ratio_genre")
kpi_pareto.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_pareto_molecules")
kpi_dynamique.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_dynamique_marche")
kpi_cout_patient.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_kpi_cout_patient")

logger.info("=== [KPI] ✅ Pôle IV sauvegardé ===")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CELLULE 8 : RAPPORT FINAL ───────────────────────────────────
dep_r = kpi_depense_reelle.first()["Depense_Reelle_2024"]
eco_g = kpi_economie_gen.first()["Economie_Generiques_Total"]
pen_g = kpi_penetration_gen.first()["Penetration_Generiques"]
tx_r  = kpi_taux_rupture.first()["Taux_Rupture_Critique"]
sc_i  = kpi_score_innovation.first()["Score_Innovation_Moyen_Pondere"]
dep_s = kpi_senior.first()["Dependance_Senior_60plus"]

print("\n" + "="*65)
print(f"  SYNTHÈSE KPI – Pharma Analytics {year}")
print("="*65)
print(f"\n  PÔLE I  – ÉCONOMIQUE")
print(f"    Dépense réelle {year}          : {float(dep_r):>14,.2f} €")
print(f"    Économie génériques estimée  : {float(eco_g):>14,.2f} €")
print(f"    Pénétration génériques       : {float(pen_g)*100:>13.2f} %")
print(f"\n  PÔLE II – SANITAIRE")
print(f"    Taux rupture critique (SMR1) : {float(tx_r)*100:>13.2f} %")
print(f"\n  PÔLE III – INNOVATION")
print(f"    Score innovation moyen ASMR  : {float(sc_i):>14.4f}")
print(f"\n  PÔLE IV – VEILLE & USAGE")
print(f"    Dépendance senior (60+)      : {float(dep_s)*100:>13.2f} %")
print("="*65)
print("\n  Tables Gold créées (20 KPI / 4 pôles) :")
tables = [
    "gold_kpi_depense_reelle","gold_kpi_projection_budget",
    "gold_kpi_economie_generiques","gold_kpi_penetration_generiques","gold_kpi_cout_atc1",
    "gold_kpi_taux_rupture_critique","gold_kpi_patients_impactes","gold_kpi_delai_penurie",
    "gold_kpi_tension_atc","gold_kpi_fiabilite_labo",
    "gold_kpi_score_innovation","gold_kpi_acces_innovation","gold_kpi_premium_innovation",
    "gold_kpi_metoo","gold_kpi_efficience_therapeutique",
    "gold_kpi_dependance_senior","gold_kpi_ratio_genre","gold_kpi_pareto_molecules",
    "gold_kpi_dynamique_marche","gold_kpi_cout_patient"
]
for t in tables:
    print(f"  ✅ {t}")
print("="*65)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
