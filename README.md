# Radar Prospection France

Ce projet optimise la recherche de clients pour les freelances.
Il se base sur la donnée officielle gouvernementale BODACC (Bulletin Officiel des Annonces Civiles et Commerciales).
Chaque jour, une liste d'entreprises témoignant d'une augmentation de capital sur les 30 derniers jours est mise à disposition pour faciliter l'acquisition client.

## Architecture

Le projet suit une architecture médaillon (bronze / silver / gold). Ce choix est motivé par 4 raisons :

- **Rejouabilité** : le bronze conserve la donnée brute, immuable. Un traitement aval bugué se corrige et se rejoue sans re-télécharger la source.
- **Séparation des responsabilités** : le silver nettoie (technique), le gold répond à une question métier.
- **Réutilisation** : les couches gold lisent le même silver, ce qui évite de refaire le nettoyage du bronze pour chaque usage.
- **Testabilité par couche** : des tests à chaque frontière, donc quand ça casse on sait exactement à quel étage corriger.

L'ensemble est orchestré par **Dagster** : l'ingestion Python et les modèles dbt vivent dans un seul DAG d'assets, déclenché chaque jour à 6 h (Europe/Paris).

```
  API BODACC (opendatasoft)
        │  asset annonces_bronze, partitionné par jour
        ▼
┌────────────────────────┐
│ BRONZE                 │  data/bronze/*.json
│ donnée brute, immuable │  la réalité telle que reçue
└───────────┬────────────┘
            ▼
┌────────────────────────┐
│ SILVER (dbt)           │
│ annonces               │  extraction & sélection des champs
│ augmentation_capital   │  filtre familleavis + "capital"
└───────────┬────────────┘
            │
     ┌──────┴───────────────────┐
     ▼                          ▼
┌──────────────────────────┐  ┌─────────────────────────┐
│ GOLD (dbt)               │  │ GOLD (dbt)              │
│ opportunite_par_         │  │ cibles_prospection      │
│ departement              │  │ « qui appeler ? »       │
│ « où prospecter ? »      │  │ grain entreprise,       │
│ agrégat par département  │  │ testé unique(siren)     │
└──────────────────────────┘  └─────────────────────────┘
```

## Quickstart

Prérequis : Docker, **Python 3.12** (l'écosystème Dagster/dbt ne supporte pas encore 3.14).

```bash
git clone https://github.com/diidouu/Radar-Prospection-France.git
cd Radar-Prospection-France

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# Le manifest dbt n'est pas versionné : Dagster en a besoin pour découvrir les modèles
cd radar && dbt parse && cd ..

# Interface Dagster sur http://localhost:3000
dagster dev -m orchestration.definitions
```

Dans l'UI : **Catalog → Materialize** pour produire un jour, **Automation** pour activer le schedule quotidien, **Runs → Backfill** pour rejouer une plage de dates.

Les deux briques restent utilisables seules : `docker compose up --build` lance l'ingestion du jour, et `cd radar && dbt build` reconstruit silver + gold.

## Choix techniques

- **DuckDB** : le volume de données tient sur une machine. DuckDB est un moteur analytique **embarqué** (in-process, aucun serveur à administrer) qui lit le JSON/Parquet nativement, soit l'analytique sans infra.
- **dbt** : transformations SQL *as code*. Les `ref` construisent le DAG et garantissent l'ordre d'exécution des modèles, avec tests de qualité et documentation auto-générée.
- **Dagster** : orchestration **orientée assets**. Le pipeline n'est pas une suite de tâches mais un graphe de données. L'ingestion bronze est partitionnée par jour, ce qui rend chaque journée rejouable indépendamment (backfill) et supprime tout calcul de date dans le code : la partition *est* la date. Les modèles dbt sont découverts automatiquement depuis le `manifest.json`, et le lien bronze → dbt passe par une source dbt reliée à l'asset Python via un `DagsterDbtTranslator`.
- **Docker** : reproductibilité, l'ingestion tourne à l'identique sur toute machine. Docker et Dagster ne sont pas concurrents mais à deux étages différents : l'un isole l'environnement, l'autre ordonnance. En production, Dagster serait lui-même conteneurisé.

## Qualité

Le pipeline est testé par dbt à chaque exécution : 7 tests déclarés dans les `schema.yaml` (unicité, non-nullité). En particulier, le test `unique` sur `siren` dans `cibles_prospection` prouve automatiquement le grain entreprise du modèle.

## Limites connues

- **Croissance du bronze** : ~19 Mo de JSON par jour, soit ~7 Go/an. Le passage au Parquet diviserait ce volume par 5 à 10 et accélérerait la lecture. C'est le premier changement à faire avant une mise en production.
- **Silver et gold en *full refresh*** : les modèles relisent l'intégralité du bronze à chaque exécution (12 s pour 26 jours aujourd'hui). Simple et sans risque de trou, mais à basculer en incrémental quand le temps de rebuild deviendra gênant.
- **Le pipeline tourne sur un poste de développement**, pas sur un serveur. Le schedule est armé mais ne se déclenche que si `dagster dev` tourne : machine éteinte, pas de tick. Le passage en production ne demande aucune modification du code, seulement un hébergement du daemon.
- **Le venv doit être activé** avant `dagster dev` : `DbtCliResource` cherche l'exécutable `dbt` dans le `PATH`. Un déploiement en service devra passer `dbt_executable` explicitement.

## Roadmap

- **Déploiement** : daemon Dagster hébergé en service (conteneur sur VM ou PaaS) pour que le schedule tourne réellement en continu.
- **Bronze en Parquet** + politique de rétention.
- **Standardisation silver** : typage explicite (dates, siren), contrôle de formats, tests `accepted_values`.
- **Enrichissement SIRENE** : croisement avec l'API Recherche d'Entreprises (secteur, effectifs, coordonnées).
- **MinIO** : dépôt du bronze dans un lake S3-compatible.
- **Restitution** : exposer `cibles_prospection` autrement qu'en table DuckDB (export CSV, ou dashboard léger).
