# RAPPORT TECHNIQUE — PROJET HEALTHTEK
## Pipeline de Données Pharmaceutiques sur Microsoft Fabric

---

**Projet :** HealthTek Pharma Analytics
**Date :** Mars 2026
**Technologies :** PySpark · Delta Lake · Microsoft Fabric · Power BI Direct Lake
**Sources de données :** BDPM (ANSM) · OpenMedic AMELI 2024

---

## TABLE DES MATIÈRES

1. [Contexte et Objectifs](#1-contexte-et-objectifs)
2. [Architecture Générale](#2-architecture-générale)
3. [Sources de Données](#3-sources-de-données)
4. [Couche Bronze — Ingestion Brute](#4-couche-bronze--ingestion-brute)
5. [Couche Silver — Qualité et Enrichissement](#5-couche-silver--qualité-et-enrichissement)
6. [Couche Gold — KPIs Analytiques](#6-couche-gold--kpis-analytiques)
7. [Modèle Sémantique Power BI](#7-modèle-sémantique-power-bi)
8. [Orchestration du Pipeline](#8-orchestration-du-pipeline)
9. [Catalogue des 20 KPIs](#9-catalogue-des-20-kpis)
10. [Piliers Qualité](#10-piliers-qualité)
11. [Synthèse et Valeur Métier](#11-synthèse-et-valeur-métier)

---

## 1. Contexte et Objectifs

### 1.1 Contexte

Le secteur pharmaceutique français produit un volume considérable de données publiques ouvertes : référentiels de médicaments, évaluations scientifiques, données de prescription et de remboursement. Ces données, bien que disponibles, sont dispersées entre plusieurs organismes (ANSM, HAS, Assurance Maladie) et nécessitent un travail important de consolidation, de nettoyage et d'enrichissement avant de pouvoir alimenter des analyses décisionnelles.

Le projet **HealthTek** répond à ce défi en construisant un pipeline de données de bout en bout sur **Microsoft Fabric**, depuis l'ingestion des fichiers sources jusqu'à la publication d'un tableau de bord Power BI interactif.

### 1.2 Objectifs

| Objectif | Description |
|----------|-------------|
| **Centraliser** | Agréger toutes les sources de données pharmaceutiques publiques en un seul Lakehouse |
| **Fiabiliser** | Appliquer un cadre qualité rigoureux en 6 piliers pour garantir l'intégrité des données |
| **Valoriser** | Calculer 20 KPIs répartis sur 4 pôles analytiques pour le pilotage métier |
| **Automatiser** | Orchestrer les mises à jour selon 4 fréquences (Quotidienne, Hebdomadaire, Mensuelle, Annuelle) |
| **Visualiser** | Exposer les données via un modèle sémantique Direct Lake connecté à Power BI |

---

## 2. Architecture Générale

### 2.1 Architecture Médaillon (Bronze / Silver / Gold)

Le pipeline suit une architecture **Médaillon** en trois couches, standard dans les lakehouses modernes :

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOURCES EXTERNES                              │
│  BDPM (ANSM)          │        OpenMedic (AMELI)                │
│  8 fichiers .txt      │        1 fichier CSV (2024)             │
└─────────────┬─────────┴──────────────┬──────────────────────────┘
              │                        │
              ▼                        ▼
┌─────────────────────────────────────────────────────────────────┐
│              COUCHE BRONZE  (tables Delta brutes)               │
│  bronze_cis_bdpm  │  bronze_cis_cip  │  bronze_cis_cip_dispo   │
│  bronze_has_smr   │  bronze_has_asmr │  bronze_cis_gener       │
│  bronze_cis_compo │  bronze_cis_cpd  │  bronze_open_medic      │
└──────────────────────────────┬──────────────────────────────────┘
                               │  6 Piliers Qualité
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              COUCHE SILVER  (données nettoyées)                 │
│  silver_cis_bdpm  │  silver_cis_cip  │  silver_dispo_spec      │
│  silver_has_smr   │  silver_has_asmr │  silver_cis_gener       │
│  silver_cis_compo │  silver_cis_cpd  │  silver_open_medic      │
│                   ▼                                             │
│         silver_health_analytics_master  (table pivot)          │
└──────────────────────────────┬──────────────────────────────────┘
                               │  20 KPIs — 4 Pôles
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              COUCHE GOLD  (KPIs calculés)                       │
│                    gold_pharma_kpis                             │
│              (partitionné par ATC1)                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │  Vues SQL sémantiques
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              MODÈLE SÉMANTIQUE POWER BI                         │
│  dim_medicament │ dim_atc │ dim_has │ dim_generique             │
│  dim_statut_disponibilite │ fact_volumes                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Plateforme | Microsoft Fabric |
| Moteur de traitement | Apache Spark (PySpark) |
| Format de stockage | Delta Lake (tables ACID) |
| Langage | Python 3 + SQL |
| Visualisation | Power BI (mode Direct Lake) |
| Mesures BI | DAX |
| Orchestration | Fabric Data Pipeline + 4 Triggers |

---

## 3. Sources de Données

### 3.1 BDPM — Base de Données Publique des Médicaments (ANSM)

La BDPM est le référentiel officiel des médicaments autorisés en France, maintenu par l'ANSM. Le projet exploite **8 fichiers** issus de cette base :

| Fichier | Table Bronze | Contenu | Fréquence |
|---------|-------------|---------|-----------|
| `CIS_bdpm.txt` | `bronze_cis_bdpm` | Spécialités pharmaceutiques (dénomination, titulaire, statut) | Mensuelle |
| `CIS_CIP_bdpm.txt` | `bronze_cis_cip` | Présentations (CIP13, prix, taux remboursement) | Quotidienne |
| `CIS_CIP_Dispo_Spec.txt` | `bronze_cis_cip_dispo_spec` | Ruptures et tensions d'approvisionnement | Hebdomadaire |
| `CIS_HAS_SMR_bdpm.txt` | `bronze_cis_has_smr` | Service Médical Rendu (SMR) évalué par la HAS | Mensuelle |
| `CIS_HAS_ASMR_bdpm.txt` | `bronze_cis_has_asmr` | Amélioration du Service Médical Rendu (ASMR) | Mensuelle |
| `CIS_GENER_bdpm.txt` | `bronze_cis_gener` | Groupes génériques (Princeps / Génériques) | Mensuelle |
| `CIS_COMPO_bdpm.txt` | `bronze_cis_compo` | Composition en substances actives + dosages | Mensuelle |
| `CIS_CPD_bdpm.txt` | `bronze_cis_cpd` | Conditions de prescription et de délivrance | Mensuelle |

**Clé universelle :** Le code **CIS** (Code Identifiant de Spécialité, 8 chiffres) est la clé primaire universelle qui relie tous les référentiels.

### 3.2 OpenMedic — Données de Prescription AMELI

Le fichier **OPEN_MEDIC_2024.CSV** provient de l'Assurance Maladie (AMELI). Il contient les données de prescription et de remboursement pour l'année **2024**, désagrégées par :

- Classe ATC (5 niveaux hiérarchiques : ATC1 à ATC5)
- Présentation CIP13
- Tranche d'âge (code numérique)
- Sexe du patient
- Région (BEN_REG) et spécialité prescripteur (PSP_SPE)

**Données de volume :**
- `BOITES` : nombre de boîtes dispensées
- `REM` : montant remboursé par l'Assurance Maladie (€)
- `BSE` : base de remboursement (€)

---

## 4. Couche Bronze — Ingestion Brute

### 4.1 Principe

La couche Bronze conserve les données **dans leur état brut**, sans transformation métier. Chaque fichier source est ingéré tel quel dans une table Delta avec deux colonnes d'audit :
- `IngestionTimestamp` : horodatage d'ingestion
- `SourceFile` : chemin du fichier source

### 4.2 Fonction d'Ingestion Générique

La fonction `ingest_to_bronze()` est réutilisable pour tous les fichiers sources. Elle gère :
- Le schéma explicite (pas d'inférence automatique)
- L'encodage des fichiers (WINDOWS-1252 par défaut)
- Le séparateur (tabulation pour BDPM, point-virgule pour OpenMedic)
- L'écriture en mode `overwrite` avec schéma évolutif (`overwriteSchema=true`)

### 4.3 Gestion de la Fréquence

L'ingestion est paramétrée par le paramètre `FREQUENCE` transmis par le pipeline Fabric :

| Fréquence | Tables ingérées |
|-----------|----------------|
| `DAILY` | `bronze_cis_cip` (prix et remboursements) |
| `WEEKLY` | `bronze_cis_cip_dispo_spec` (ruptures ANSM) |
| `MONTHLY` | 6 tables BDPM (spécialités, SMR, ASMR, génériques, composition, CPD) |
| `ANNUAL` | `bronze_open_medic` (volumes prescription 2024) |
| `ALL` | Toutes les tables |

---

## 5. Couche Silver — Qualité et Enrichissement

### 5.1 Cadre Qualité en 6 Piliers

Le passage Bronze → Silver applique un cadre qualité systématique en **6 piliers** via la fonction orchestratrice `apply_healthtek_quality()` :

| Pilier | Fonction | Description |
|--------|----------|-------------|
| **0** | `standardize_columns()` | Normalisation des noms de colonnes en PascalCase (CIS, CIP, ATC conservés en majuscules) |
| **1** | `handle_technical_normalization()` | Nettoyage des caractères spéciaux : guillemets typographiques, espaces insécables |
| **2** | `apply_default_labels()` | Remplacement des valeurs nulles/vides par des valeurs par défaut métier |
| **3** | `apply_healthtek_aesthetic()` | Capitalisation sentence case, nettoyage balises HTML résiduelle, correction apostrophes |
| **4** | `apply_healthtek_scoring()` | Conversion des libellés SMR/ASMR en scores numériques ordinaux |
| **5** | `convert_healthtek_types()` | Typage strict : Integer, Double, Date (formats multiples gérés), pourcentages |
| **6** | `apply_healthtek_padding()` | Padding des identifiants : CIS (8 chiffres), CIP13 (13 chiffres), CIP7 (7 chiffres) |

### 5.2 Scoring SMR / ASMR

La conversion des évaluations HAS en scores numériques permet les calculs analytiques :

**Score SMR (Service Médical Rendu) :**
| Valeur | Score | Signification |
|--------|-------|---------------|
| Important | 1 | Médicament vital |
| Modéré | 2 | Utilité médicale significative |
| Faible | 3 | Utilité médicale limitée |
| Insuffisant | 4 | Non remboursable |

**Score ASMR (Amélioration du SMR) :**
| Valeur | Score | Signification |
|--------|-------|---------------|
| ASMR I | 1 | Amélioration majeure |
| ASMR II | 2 | Amélioration importante |
| ASMR III | 3 | Amélioration modérée |
| ASMR IV | 4 | Amélioration mineure |
| ASMR V | 5 | Aucune amélioration (Me-Too) |

### 5.3 Démographie OpenMedic

La fonction `transform_openmedic_demographics()` traduit les codes numériques en libellés analytiques :

- **Tranches d'âge** : 0-19 ans / 20-59 ans / 60 ans et + (avec ordre de tri)
- **Sexe** : Homme (1) / Femme (2) / Inconnu
- **Extraction ATC1** : dérivé du code ATC5 (premier caractère)

### 5.4 Table Master Silver

Le **Master Silver** (`silver_health_analytics_master`) est la table pivot centrale qui consolide toutes les dimensions. Son grain est : **(CIP13 × CIS × Age_Label × Sexe_Label)**.

**Jointures effectuées :**
```
fact_om (OpenMedic, grain CIP13×Age×Sexe)
  ├── LEFT JOIN dim_cip       (sur CIP13)
  ├── LEFT JOIN fact_cis      (sur CIS)
  ├── LEFT JOIN dim_dispo     (sur CIS+CIP13)
  ├── LEFT JOIN dim_smr       (sur CIS, dernier avis par fenêtre temporelle)
  ├── LEFT JOIN dim_asmr      (sur CIS, dernier avis par fenêtre temporelle)
  ├── LEFT JOIN dim_gener     (sur CIS)
  ├── LEFT JOIN dim_compo     (sur CIS, agrégation substances+dosages)
  └── LEFT JOIN dim_cpd       (sur CIS, agrégation conditions prescription)
```

**Point technique clé — Fenêtre temporelle SMR/ASMR :** Pour éviter les doublons dus à l'historique des évaluations HAS, seul le **dernier avis** (DateAvis la plus récente) est conservé via une `Window.partitionBy("CIS").orderBy(F.col("DateAvis").desc())`.

### 5.5 Validation Qualité Post-Silver

Une validation automatique est exécutée après chaque table Silver via `validate_silver_table()` :
- Vérification que la table n'est pas vide
- Contrôle du taux de nullité sur les colonnes critiques :
  - CIS : seuil maximal de 1% de nulls
  - Autres colonnes clés : seuil de 5%
- **Arrêt du pipeline** si un seuil est dépassé

---

## 6. Couche Gold — KPIs Analytiques

### 6.1 Principe

La couche Gold calcule **20 KPIs** pré-agrégés à partir du Master Silver. Le grain de la table Gold reste fin : **(CIS × CIP13 × Age_Label × Sexe_Label)**, les KPIs étant calculés au niveau CIS puis rejoints sur ce grain.

### 6.2 Flags Métier

Avant le calcul des KPIs, 11 **flags booléens** (0/1) sont créés pour simplifier les filtres :

| Flag | Condition |
|------|-----------|
| `IsRupture` | CodeStatut contient "rupture" |
| `IsVital` | ValeurSmr_Score = 1 (SMR Important) |
| `IsRuptureCritique` | IsRupture = 1 ET IsVital = 1 |
| `IsGenerique` | TypeGenerique > 0 |
| `IsPrinceps` | TypeGenerique = 0 |
| `IsAsmrProgres` | ValeurAsmr_Score entre 1 et 3 |
| `IsMeToo` | ValeurAsmr_Score = 5 (ASMR V) |
| `IsSenior` | Age_Label = "60 ans et +" |
| `IsFemme` | Sexe_Label = "Femme" |
| `IsHomme` | Sexe_Label = "Homme" |

### 6.3 Organisation par Pôle

Les KPIs sont organisés en **4 pôles analytiques** (voir section 9 pour le catalogue complet).

### 6.4 Partitionnement

La table Gold est **partitionnée par ATC1** (classe thérapeutique principale, A à Z). Ce partitionnement optimise les performances des requêtes Power BI qui filtrent fréquemment par classe thérapeutique.

---

## 7. Modèle Sémantique Power BI

### 7.1 Vues SQL Sémantiques

Le notebook `04_NB_Semantic_Views` crée **6 vues SQL** exposées au modèle Direct Lake :

| Vue | Clé | Rôle |
|-----|-----|------|
| `dim_medicament` | CIS | Descriptif complet : dénomination, forme, titulaire, statut marché |
| `dim_atc` | ATC5 | Hiérarchie thérapeutique ATC1→ATC5, flag Pareto |
| `dim_has` | CIS | Évaluations HAS : SMR + ASMR avec labels enrichis |
| `dim_generique` | CIS | Groupes génériques : Princeps / Générique / Substituable |
| `dim_statut_disponibilite` | CodeStatut | Référentiel ruptures : disponible / tension / rupture |
| `fact_volumes` | CIS × CIP13 × Age × Sexe | Table de faits principale avec tous les KPIs |

### 7.2 Schéma Étoile

```
                    dim_medicament
                         │ CIS
                         │
dim_statut_disponibilite ─── fact_volumes ─── dim_has
     CodeStatut          │       │CIS         CIS
                         │       │
                    dim_generique│       dim_atc
                         CIS     │ATC5    ATC5
```

**Règles de relation :**
- Cardinalité many-to-one (Fact → Dim)
- Sens de filtre unique (Dim → Fact)
- Pas de cross-filter bidirectionnel

### 7.3 Mesures DAX

Le fichier `05_DAX_Measures_PowerBI.dax` définit **20 mesures DAX** organisées en 4 dossiers, plus **6 mesures auxiliaires** pour les visuels standards :

- `[Dépense Réelle 2024 (€)]`
- `[Projection Budget 2026 (€)]`
- `[Économie Génériques (€)]`
- `[Taux Rupture Critique (%)]`
- `[Score Innovation Moyen (ASMR)]`
- `[Dépendance Senior (%)]`
- etc.

### 7.4 Pages Power BI Recommandées

| Page | Visuels clés |
|------|-------------|
| **Vue Exécutive** | 4 KPIs cards, courbe Rem par ATC1, slicers globaux |
| **Analyse Économique** | Tableau Princeps/Génériques, Bar chart coûts ATC1, Scatter Prix/Volume |
| **Monitoring Ruptures** | Matrix Titulaire×Statut, Timeline délai pénurie, Drillthrough ANSM |
| **Innovation HAS** | Donut Me-Too vs Innovation, Bar SMR Efficience, Scatter ASMR vs Volume |
| **Veille & Usage** | Pyramide âges, Ratio Genre, Courbe Pareto 80/20 |

---

## 8. Orchestration du Pipeline

### 8.1 Notebook Orchestrateur

Le notebook `06_NB_Pipeline_Orchestrator` coordonne l'exécution séquentielle des 4 notebooks via `notebookutils.notebook.run()` :

```
[1/4] Bronze  (timeout: 30 min)
      ↓ (arrêt si erreur)
[2/4] Silver + Master  (timeout: 60 min)
      ↓ (arrêt si erreur)
[3/4] Gold KPIs  (seulement si ANNUAL ou ALL — timeout: 60 min)
      ↓ (non bloquant si erreur)
[4/4] Vues Sémantiques  (seulement si ANNUAL ou ALL — timeout: 10 min)
```

**Gestion des erreurs :**
- Les étapes Bronze et Silver sont **bloquantes** : une erreur arrête immédiatement le pipeline
- L'étape Vues Sémantiques est **non bloquante** : les données Gold restent valides même si la création des vues échoue

### 8.2 Triggers Automatiques

4 triggers Fabric sont configurés selon les fréquences de mise à jour des sources :

| Trigger | Schedule | Paramètre | Source mise à jour |
|---------|----------|-----------|-------------------|
| `trigger_daily_cis_cip` | Chaque jour à 06:00 UTC | `DAILY` | Prix et remboursements CIP |
| `trigger_weekly_dispo` | Chaque lundi à 05:00 UTC | `WEEKLY` | Ruptures ANSM |
| `trigger_monthly_bdpm` | 1er du mois à 04:00 UTC | `MONTHLY` | Référentiel BDPM complet |
| `trigger_annual_openmedic` | Manuel / 1er janvier à 03:00 UTC | `ANNUAL` | OpenMedic + recalcul Gold |

---

## 9. Catalogue des 20 KPIs

### Pôle I — Analyse Économique (KPIs 1 à 5)

| # | KPI | Formule | Usage |
|---|-----|---------|-------|
| 1 | **Dépense Réelle 2024** | `SUM(Rem)` | Budget réel remboursé par l'AM |
| 2 | **Projection Budget 2026** | `SUM(Boites × Prix × TauxRemb)` | Anticipation budgétaire |
| 3 | **Économie Génériques** | `ΣPrinceps - ΣGénériques (×Boites×Prix)` | Gain potentiel de substitution |
| 4 | **Pénétration Génériques** | `BoitesGénériques / BoitesTotal` | Part de marché générique |
| 5 | **Coût Moyen ATC1** | `Bse_Total / NbCIS` par classe | Coût moyen par famille thérapeutique |

### Pôle II — Monitoring Sanitaire (KPIs 6 à 10)

| # | KPI | Formule | Usage |
|---|-----|---------|-------|
| 6 | **Taux Rupture Critique** | `NbRupturesVitaux / NbMédicamentsVitaux` | Risque patient critique |
| 7 | **Patients Impactés** | `SUM(Boites)` where IsRupture=1 | Volume de patients exposés |
| 8 | **Délai Moyen Pénurie** | `AVG(DateRemise - DateDebut)` en jours | Durée moyenne des ruptures |
| 9 | **Indice Tension ATC** | `COUNT(CIS)` en rupture par ATC | Pression par classe thérapeutique |
| 10 | **Fiabilité Labo** | `1 - (NbRuptures / NbCIS_total)` | Score de fiabilité du laboratoire |

### Pôle III — Innovation HAS (KPIs 11 à 15)

| # | KPI | Formule | Usage |
|---|-----|---------|-------|
| 11 | **Score Innovation Moyen** | `Σ(AsmrScore × Boites) / ΣBoites` | Innovation pondérée par volume |
| 12 | **Taux Accès Innovation** | `BoitesASMR_I-III / BoitesTotal` | Part des médicaments innovants |
| 13 | **Premium Innovation** | `Prix_ASMR1-3 / Prix_ASMR4-5` | Surcoût des médicaments innovants |
| 14 | **Part Me-Too** | `NbMeToo / NbCIS_total` | Part des ASMR V (aucune innovation) |
| 15 | **Efficience Thérapeutique** | `ΣBoites / ΣScoreSMR` | Volumes consommés par niveau SMR |

### Pôle IV — Veille & Usage (KPIs 16 à 20)

| # | KPI | Formule | Usage |
|---|-----|---------|-------|
| 16 | **Dépendance Senior** | `Boites_60+ / BoitesTotal` | Part de consommation des seniors |
| 17 | **Ratio de Genre** | `BoitesFemmes / BoitesHommes` | Équilibre de genre dans la prescription |
| 18 | **Pareto 80/20** | Rang cumulé ATC5 par volume | Identification des 20% molécules = 80% volume |
| 19 | **Coût Patient Moyen** | `Bse / Boites` | Coût net par unité dispensée |
| 20 | **Dynamique Marché** | CIS dans BDPM absents d'OpenMedic | Nouveaux produits autorisés non encore vendus |

---

## 10. Piliers Qualité

### 10.1 Normalisation des Identifiants

Le padding des identifiants garantit l'intégrité des jointures :
- **CIS** : 8 chiffres avec zéros à gauche (ex. `0060027` → `00060027`)
- **CIP13** : 13 chiffres (code barre européen)
- **CIP7** : 7 chiffres (ancien code)
- **ATC5** : Majuscules et trim

### 10.2 Déduplication

La stratégie de déduplication est adaptée au grain de chaque table :
- `silver_cis_cip` : déduplication sur CIP13
- `silver_open_medic` : déduplication sur **(CIP13, Age, Sexe)** — grain réel de la table
- `silver_dispo_spec` : déduplication sur (CIS, CIP7)
- Tables BDPM : déduplication sur CIS

### 10.3 Gestion des Valeurs Manquantes

| Colonne | Valeur par défaut |
|---------|------------------|
| StatutBdm | "Non Renseigné" |
| CodeStatut | "Disponible" |
| Titulaire | "Inconnu" |
| LibelleStatut | "Disponible" |

---

## 11. Synthèse et Valeur Métier

### 11.1 Ce que le Projet Produit

Le projet HealthTek délivre un **système de veille pharmaceutique complet** couvrant 4 dimensions clés :

1. **Économique** : suivi budgétaire de l'Assurance Maladie, identification des leviers d'économie via la substitution générique, projection 2026
2. **Sanitaire** : surveillance en temps quasi-réel des ruptures d'approvisionnement, identification des médicaments vitaux menacés, traçabilité des pénuries
3. **Innovation** : scoring objectif des médicaments via les évaluations HAS (SMR/ASMR), identification des me-too vs vrais progrès thérapeutiques
4. **Usage** : analyse démographique de la prescription, détection des molécules à fort impact (Pareto), suivi de la dynamique marché

### 11.2 Points Forts Techniques

- **Architecture scalable** : le pipeline gère des millions de lignes grâce à Spark et Delta Lake
- **Qualité certifiée** : 6 piliers de qualité + validation automatique empêchent la publication de données corrompues
- **Mises à jour différentielles** : le paramètre FREQUENCE évite de recalculer l'intégralité du pipeline à chaque mise à jour
- **Modèle sémantique optimisé** : partitionnement ATC1, Direct Lake, schéma étoile pour des performances Power BI maximales
- **Maintenabilité** : fonctions réutilisables, logging structuré, gestion d'erreurs explicite

### 11.3 Limites et Perspectives

| Limite actuelle | Perspective d'amélioration |
|----------------|---------------------------|
| Données OpenMedic annuelles uniquement | Intégration des données SNIIRAM mensuelles |
| Pas de données historiques multi-années | Ajout d'une dimension Date pour le suivi longitudinal |
| ATC libellés reconstruits manuellement | Jointure avec le référentiel ATC complet de l'OMS |
| Projection 2026 basée sur une croissance linéaire | Modèle prédictif ML intégré dans Fabric |

---

*Rapport généré automatiquement à partir de l'analyse des notebooks HealthTek — Mars 2026*
