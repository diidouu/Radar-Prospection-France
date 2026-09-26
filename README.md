# Radar Prospection France

Ce projet optimise la recherche de clients pour les freelances.
Il se base sur la donnée officielle gouvernementale BODACC (Bulletin Officiel des Annonces Civiles et Commerciales).
Chaque jour, une liste d'entreprises témoignant d'une augmentation de capital sur les 30 derniers jours est mise à disposition pour faciliter l'acquisition client.

## Architecture

Le projet suit une architecture médaillon (bronze / silver / gold). Ce choix est motivé par 4 raisons :

- **Rejouabilité** : le bronze conserve la donnée brute, immuable — un traitement aval bugué se corrige et se rejoue sans re-télécharger la source.
- **Séparation des responsabilités** : le silver nettoie (technique), le gold répond à une question métier.
- **Réutilisation** : les couches gold lisent le même silver, ce qui évite de refaire le nettoyage du bronze pour chaque usage.
- **Testabilité par couche** : des tests à chaque frontière — quand ça casse, on sait exactement à quel étage corriger.

```
  API BODACC (opendatasoft)
        │  ingest.py — quotidien, Docker
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

Prérequis : Docker, Python 3.12+.

```bash
git clone https://github.com/diidouu/Radar-Prospection-France.git
cd Radar-Prospection-France

# Bronze : ingestion du jour (via Docker)
docker compose up --build

# Environnement dbt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# Silver + gold : transformations et tests
cd radar
dbt run && dbt test
```

## Choix techniques

- **DuckDB** — le volume de données tient sur une machine ; DuckDB est un moteur analytique **embarqué** (in-process, aucun serveur à administrer) qui lit le JSON/Parquet nativement : l'analytique sans infra.
- **dbt** — transformations SQL *as code* : les `ref` construisent le DAG et garantissent l'ordre d'exécution des modèles, avec tests de qualité et documentation auto-générée.
- **Docker** — reproductibilité : l'ingestion tourne à l'identique sur toute machine.

## Qualité

Le pipeline est testé par dbt à chaque exécution : 7 tests déclarés dans les `schema.yaml` (unicité, non-nullité). En particulier, le test `unique` sur `siren` dans `cibles_prospection` prouve automatiquement le grain entreprise du modèle.

## Roadmap

- **Dagster** : orchestration du pipeline complet (ingestion → dbt run → dbt test), schedule quotidien et backfills par date.
- **Standardisation silver** : typage explicite (dates, siren), contrôle de formats, tests `accepted_values`.
- **Ingestion paramétrable** : date cible en argument pour rejouer un jour donné (préparation des partitions Dagster).
- **Enrichissement SIRENE** : croisement avec l'API Recherche d'Entreprises (secteur, effectifs, coordonnées).
- **MinIO** : dépôt du bronze dans un lake S3-compatible.
