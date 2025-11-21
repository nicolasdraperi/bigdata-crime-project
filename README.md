
# Criminalité à Los Angeles – Data Lake & Dashboard

## Contexte & choix du dataset

Ce projet s’appuie sur un dataset de la police de Los Angeles contenant plus d’un million de déclarations de crimes.  
Chaque ligne correspond à un incident, avec notamment :

- Date et heure de survenue du crime  
- Zone géographique et nom de l’area  
- Type de crime (code + description)  
- Gravité du crime (`Part 1-2`, de 1 à 2)  
- Lieu de commission (domicile, rue, commerce…)  
- Caractéristiques de la victime (âge, sexe, origine)  
- Coordonnées GPS (LAT / LON)

Pourquoi ce dataset ?

- **Dimension temporelle + géographique** → idéal pour faire des heatmaps, des top zones  
- **Vraies questions métier** possibles (sécurité, prévention, contextes à risque)  
- **Données publiques** et réalistes, facilement justifiables

---

## Questions auxquelles le projet répond

Le projet vise à répondre à plusieurs questions analytiques :

## Questions analytiques adressées par le projet

1. **Comment varie le nombre moyen de crimes selon les zones de Los Angeles et la période d’observation ?**  
   - Quelles zones enregistrent en moyenne le plus de crimes **par jour** ?  
   - Quelles zones restent les plus actives lorsqu’on regarde **par semaine** ou **par mois** ?  
   - Observe-t-on des zones “chroniquement” plus criminogènes, quel que soit l’horizon temporel ?

2. **Quelles sont les zones les plus à risque sur l’ensemble de la période étudiée ?**  
   - Quelles sont les **5 zones** qui concentrent le plus grand nombre de crimes au total ?  
   - La répartition des crimes est-elle très concentrée sur quelques zones, ou plutôt diffuse sur l’ensemble de la ville ?  

3. **Comment le volume de criminalité évolue-t-il dans le temps ?**  
   - Le nombre de crimes **augmente-t-il ou diminue-t-il** d’année en année à Los Angeles ?  
   - Certaines **zones spécifiques** suivent-elles une tendance différente du reste de la ville (amélioration ou dégradation) ?  
   - La part des crimes les plus graves (Part 2) **évolue-t-elle différemment** des crimes moins graves (Part 1) ?

4. **Comment les crimes se répartissent-ils dans l’espace et le temps à l’échelle de la ville ?**  
   - Quelles zones géographiques apparaissent comme des **“points chauds” récurrents** sur la carte de Los Angeles ?  
   - Observe-t-on des **variations saisonnières ou mensuelles** dans certaines zones (pics à certains mois ou périodes) ?  
   - Certains quartiers sont-ils particulièrement touchés à des périodes précises de l’année ?

5. **Quels sont les contextes dans lesquels le risque d’incident est significativement plus élevé que la moyenne ?**  
   - Pour quelles combinaisons de **zone × tranche horaire × type de lieu × tranche d’âge × sexe de la victime** observe-t-on un nombre d’incidents anormalement élevé ?  
   - Peut-on identifier des “scénarios typiques” de haut risque (par exemple, tel type de lieu, à telle heure, avec tel profil de victime) ?  
   - Ces contextes à haut risque concernent-ils majoritairement certains types de victimes (par exemple certaines tranches d’âge ou un sexe en particulier) ?

---

## Architecture du projet

L’architecture suit une logique de **trois couches** : ingestion, persistance, insight.

### 1. Ingestion

- Téléchargement des données brutes au format CSV
- Stockage local dans `data/raw/crime/crime_raw.csv`
- Envoi du CSV brut dans HDFS dans la zone **RAW** :

  - `/datalake/raw/crime/crime_raw.csv`

### 2. Persistance (Data Lake HDFS)

Les données sont stockées dans HDFS avec trois zones :

- **RAW**  
  - `/datalake/raw/crime`  
  - CSV brut tel que téléchargé

- **CURATED** (données nettoyées)  
  - `/datalake/curated/crime`  
  - Nettoyage des dates, des types, filtrage des lignes invalides…  
  - Partitionnement par année d’occurrence du crime

- **ANALYTICS** (données agrégées)  
  - `/datalake/analytics/crime/...`  
  - Exemples de dossiers générés :  
    - `avg_by_area_day`  
    - `avg_by_area_week`  
    - `avg_by_area_month`  
    - `total_by_area`  
    - `crimes_per_year`  
    - `crimes_per_year_area`  
    - `crimes_per_year_severity`  
    - `crimes_per_year_area_severity`  
    - `crime_heatmap_monthly`  
    - `context_risk_scores`  
    - `high_risk_contexts`  

### 3. Insight (calcul & visualisation)

- **Cluster Spark** (via Docker) pour les traitements distribués :
  - `spark-master`
  - `spark-worker`
- **Hadoop HDFS** :
  - `namenode`
  - `datanode`
- **Dashboard Streamlit** :
  - Container dédié pour l’application web de visualisation

### 4. Scripts principaux

- `src/ingestion/download_crime_data.py`  
  Télécharge le CSV brut en local.
- `src/processing/clean_crime_data.py`  
  Nettoie les données RAW et les écrit en CURATED dans HDFS.
- `src/processing/aggregate_crime_stats.py`  
  Calcule toutes les agrégations nécessaires à la visualisation.
- `src/processing/detect_high_risk_contexts.py`  
  Détecte les scénarios de contexte à haut risque.
- `src/pipeline/run_pipeline.py`  
  Orchestrateur qui enchaîne l’ensemble des étapes ci-dessus.

---

## Pipeline end-to-end

Le script `src/pipeline/run_pipeline.py` exécute la pipeline complète :

1. **Build et démarrage de l’infrastructure Docker**  
   - `docker compose build`  
   - `docker compose up -d`  
   - Démarrage de HDFS, de Spark et du dashboard

2. **Téléchargement des données brutes**  
   - Lancement de `download_crime_data.py` depuis la machine hôte  
   - Sauvegarde dans `data/raw/crime/crime_raw.csv`

3. **Chargement dans HDFS (zone RAW)**  
   - Copie du CSV dans le container `namenode`  
   - Création du dossier `/datalake/raw/crime` dans HDFS  
   - `hdfs dfs -put -f` du CSV dans HDFS

4. **Nettoyage des données (RAW → CURATED)**  
   - Lancement de `clean_crime_data.py` via `spark-submit` sur le cluster

5. **Agrégations analytiques (CURATED → ANALYTICS)**  
   - Lancement de `aggregate_crime_stats.py` via `spark-submit`  
   - Création de tous les jeux de données analytiques utilisés par le dashboard

6. **Détection des contextes à haut risque**  
   - Lancement de `detect_high_risk_contexts.py` via `spark-submit`  
   - Écriture des datasets `context_risk_scores` et `high_risk_contexts` dans HDFS

À la fin de la pipeline, le dashboard Streamlit est prêt à consommer les données depuis HDFS.

---

## Prérequis

Pour exécuter le projet, il faut :

- **Git**
- **Python** 3.9+ (recommandé 3.10)
- **Docker** et **Docker Compose**
- Une connexion Internet (au moins lors du premier lancement, pour télécharger le dataset)

---

## Installation & lancement

Depuis un terminal :

```bash
git clone https://github.com/nicolasdraperi/bigdata-crime-project
cd ProjetCrime
```

Créer et activer un environnement virtuel (exemple Windows) :

```bash
python -m venv env
env\Scripts\activate
```

Sous Linux / macOS :

```bash
python -m venv env
source env/bin/activate
```

Installer les dépendances Python nécessaires côté host :

```bash
pip install -r requirements.txt
```

Lancer ensuite la pipeline complète :

```bash
python src/pipeline/run_pipeline.py
```

Ce script :

- build et démarre tous les containers Docker
- télécharge le dataset
- l’envoie dans HDFS
- nettoie et agrège les données avec Spark
- détecte les scénarios à haut risque

Une fois terminé, le dashboard est disponible à l’adresse :

> http://localhost:8501

---

## Utilisation du dashboard

L’application Streamlit propose plusieurs pages accessibles via la navigation à gauche.

### 1. Moyennes par zone

- Choix de la période d’agrégation : **Jour / Semaine / Mois**
- Affichage :
  - tableau des moyennes de crimes par **AREA NAME**
  - graphiques interactifs (Plotly) pour explorer les zones les plus actives

### 2. Top 5 zones les plus à risque

- Classement des zones selon le volume total de crimes
- Visualisation bar chart des 5 zones les plus à risque
- Permet d’identifier rapidement les “hotspots” de criminalité

### 3. Évolution annuelle

- Évolution du nombre de crimes **par année**
- Possibilité de :
  - suivre la tendance globale sur toute la ville
  - comparer par **zone**
  - différencier par **gravité** (`Part 1-2`) pour voir si les crimes graves augmentent ou diminuent

### 4. Heatmap spatio-temporelle

- Carte Leaflet de Los Angeles (via Folium)
- Paramètres :
  - année
  - mois
  - éventuellement filtrage par zone
- Affiche une heatmap des crimes sur la ville pour le mois choisi

### 5. Scénarios à haut risque

- Lecture des données `high_risk_contexts` générées par Spark
- Filtres disponibles :
  - zone (AREA NAME)
  - tranche horaire (nuit, matin, après-midi, soirée)
  - type de lieu (domicile, rue, commerce…)
  - tranche d’âge de la victime
  - sexe de la victime
  - niveau de risque ( Élevé / Très élevé)
- Affiche :
  - un résumé (nombre de scénarios, crimes max, z-score max)
  - un tableau détaillé des scénarios filtrés
  - un bar chart des scénarios les plus critiques

---

## Partie Analytique – Détection de contextes à haut risque

La détection de scénarios à haut risque s’appuie sur une approche statistique interprétable :

1. **Filtrage des victimes humaines**  
   - On ne conserve que les incidents où la victime est humaine  
     (âge > 0 ou sexe M/F).  
   - Les crime sur entreprises / organisations sont exclus de cette partie.

2. **Construction des contextes**  
   Pour chaque incident, on dérive :

   - `zone` : `AREA NAME`  
   - `time_slot` : tranche horaire (0–5, 6–11, 12–17, 18–23)  
   - `place_type` : `Premis Desc`  
   - `age_range` : 0–17, 18–29, 30–44, 45–64, 65+  
   - `sex_norm` : M, F, Autre, Inconnu  

3. **Agrégation par contexte**  
   - GroupBy sur cette combinaison  
   - Calcul de `n_crimes` = nombre d’incidents pour chaque contexte

4. **Scoring (z-score)**  
   - On calcule la moyenne globale et l’écart-type des `n_crimes`  
   - Pour chaque contexte :  
     `z_score = (n_crimes - moyenne) / écart_type`

5. **Sélection des contextes à haut risque**  
   - Seuils :
     - `n_crimes >= 50` (pour éviter les cas trop rares)
     - `z_score >= 2` (plus de 2 écarts-types au-dessus de la moyenne)
   - Les contextes qui passent ce filtre sont écrits dans `high_risk_contexts` et affichés dans le dashboard.

Cette méthode n’est pas du machine learning complexe, mais elle apporte une **analyse des risques** tout en restant simple à expliquer.

---

## Limites & pistes d’amélioration

- Une seule ville (Los Angeles) : pas de comparaison inter-villes  
- Les données sont traitées en **batch**, pas de temps réel  
- Le modèle de risque est purement statistique (z-score), sans apprentissage supervisé  
- Certaines colonnes riches (`Crm Cd Desc`, `Vict Descent`, etc.) pourraient être exploitées pour aller plus loin (clustering, profiling, etc.)