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
master = (
    spark.read.format("delta")
    .load(f"{TABLES_ROOT}/silver_health_analytics_master")
    .withColumn("CIP13", F.lpad(F.trim(F.col("CIP13")), 13, "0"))
)

total = master.count()
print(f"\nTotal lignes master : {total:,}")

# CHECK 1 : TAUX DE REMPLISSAGE ───────────────────────────────
print("\n" + "="*60)
print("  CHECK 1 – Taux de remplissage par jointure")
print("="*60)

checks = {
    "CIS"                  : "silver_cis_bdpm",
    "LibellePresentation"  : "silver_cis_cip (CIP13 → Prix)",
    "PrixMedicament"       : "silver_cis_cip (Prix)",
    "TauxRemboursement"    : "silver_cis_cip (Taux)",
    "Titulaire"            : "silver_cis_bdpm (Labo)",
    "EtatCommercialisation": "silver_cis_bdpm (État)",
    "CodeStatut"           : "silver_dispo_spec (Ruptures)",
    "ValeurSmr"            : "silver_has_smr",
    "ValeurAsmr"           : "silver_has_asmr",
    "TypeGenerique"        : "silver_cis_gener",
    "CompositionSubstances": "silver_cis_compo",
}

resultats = []
for col, jointure in checks.items():
    if col in master.columns:
        nb  = master.filter(F.col(col).isNotNull()).count()
        pct = round(nb / total * 100, 1)
        s   = "✅" if pct >= 50 else "⚠️ " if pct >= 10 else "❌"
        resultats.append((s, col, jointure, nb, pct))
    else:
        resultats.append(("❌", col, jointure, 0, 0.0))

print(f"\n{'St':<4} {'Colonne':<28} {'Remplis':>10} {'%':>6}  Jointure")
print("-"*80)
for s, col, jointure, nb, pct in resultats:
    print(f"  {s}  {col:<28} {nb:>10,} {pct:>5.1f}%  {jointure}")

# CHECK 2 : INTÉGRITÉ DES CLÉS ────────────────────────────────
print("\n" + "="*60)
print("  CHECK 2 – Intégrité CIP13 / CIS")
print("="*60)

for key, length in [("CIP13", 13), ("CIS", 8)]:
    nb_null = master.filter(F.col(key).isNull()).count()
    nb_bad  = master.filter(F.length(F.col(key)) != length).count()
    nb_ok   = total - nb_null - nb_bad
    print(f"\n  {key} ({length} chiffres) :")
    print(f"    ✅ Valides  : {nb_ok:>10,}")
    print(f"    ⚠️  Longueur : {nb_bad:>10,}")
    print(f"    ❌ Nuls     : {nb_null:>10,}")

# CHECK 3 : DOUBLONS CIP13 ────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 3 – Doublons sur CIP13 (fan-out)")
print("="*60)

nb_distinct = master.select("CIP13").distinct().count()
nb_doublons = total - nb_distinct
print(f"\n  Lignes totales  : {total:>10,}")
print(f"  CIP13 distincts : {nb_distinct:>10,}")
print(f"  Doublons        : {nb_doublons:>10,}")

if nb_doublons > 0:
    print("\n  ⚠️  Top 5 CIP13 dupliqués :")
    master.groupBy("CIP13").count().filter(F.col("count") > 1) \
        .orderBy(F.desc("count")).show(5, truncate=False)
else:
    print("  ✅ Aucun doublon")

# CHECK 4 : COHÉRENCE MÉTIER ──────────────────────────────────
print("\n" + "="*60)
print("  CHECK 4 – Cohérence métier")
print("="*60)

m = master.agg(
    F.sum("Boites").alias("total_boites"),
    F.sum("Rem").alias("total_rem"),
    F.sum("Bse").alias("total_bse"),
    F.min("PrixMedicament").alias("prix_min"),
    F.max("PrixMedicament").alias("prix_max"),
    F.avg("PrixMedicament").alias("prix_moy"),
    F.countDistinct("ATC1").alias("nb_atc1"),
    F.countDistinct("CIS").alias("nb_cis"),
).first()

print(f"\n  Boîtes totales   : {m['total_boites']:>15,.0f}")
print(f"  Montant remboursé: {m['total_rem']:>15,.2f} €")
print(f"  Base remboursement:{m['total_bse']:>14,.2f} €")
print(f"  Prix min/moy/max : {m['prix_min']:>7.2f} / {m['prix_moy']:>7.2f} / {m['prix_max']:>7.2f} €")
print(f"  Classes ATC1     : {m['nb_atc1']:>15,}")
print(f"  Spécialités CIS  : {m['nb_cis']:>15,}")

print("\n  " + ("❌ Rem > Bse !" if m["total_rem"] > m["total_bse"] else "✅ Rem <= Bse"))
print("  " + ("❌ Prix négatif !" if m["prix_min"] < 0 else "✅ Prix positifs"))

# CHECK 5 : ORPHELINS ─────────────────────────────────────────
print("\n" + "="*60)
print("  CHECK 5 – Orphelins Open Medic sans BDPM")
print("="*60)

nb_orphelins = master.filter(F.col("CIS").isNull()).count()
pct_orphelins = round(nb_orphelins / total * 100, 2)
print(f"\n  CIP13 sans CIS : {nb_orphelins:>10,}  ({pct_orphelins}%)")

if nb_orphelins > 0:
    master.filter(F.col("CIS").isNull()).select("CIP13","Boites","Rem").show(5, truncate=False)
else:
    print("  ✅ Aucun orphelin")

# RÉSUMÉ ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("  RÉSUMÉ – Actions recommandées")
print("="*60)

remplis_ko = [r for r in resultats if r[4] < 10]
if remplis_ko:
    for r in remplis_ko:
        print(f"  ❌ {r[1]} ({r[4]}%) → vérifier bronze correspondant")
if nb_doublons > 0:
    print(f"  ⚠️  {nb_doublons:,} doublons → dropDuplicates(['CIP13']) dans 02_NB_Silver")
if pct_orphelins > 10:
    print(f"  ⚠️  {pct_orphelins}% orphelins → vérifier bronze_cis_cip")

if not remplis_ko and nb_doublons == 0 and pct_orphelins <= 10:
    print("\n  ✅ Master cohérent – tu peux lancer 03 et 04")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
