# DataStore360

Projet d'apprentissage de l'ingenierie de donnees (data engineering) : construction pas a pas d'un pipeline ETL complet sur le jeu de donnees Superstore.

## Stack

| Composant | Role |
|---|---|
| **PostgreSQL 16** | Entrepot de donnees (schemas `staging` et `core`) |
| **pgAdmin 4** | Interface de gestion de la base |
| **Apache Airflow 3** | Orchestration des pipelines (mode standalone) |
| **Python + Pandas + uv** | Extraction, nettoyage et transformation |

## Demarrer l'environnement

Infrastructure via Docker Compose :

```bash
docker compose up -d
```

| Service | URL | Identifiants |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@example.com / password |
| PostgreSQL | localhost:5432 | data_user / my_password |

Environnement Python (recree automatiquement avec `uv` depuis `pyproject.toml` et `uv.lock`) :

```bash
uv sync
```

## Structure du projet

```
data/           # donnees brutes (raw) et transformees (processed)
notebooks/      # exploration et etapes de traitement (Jupyter)
src/            # fonctions reutilisables (extraction, nettoyage, chargement, connexion base)
dags/           # DAG Airflow
include/sql/    # scripts SQL : schemas staging et core
reports/        # rapport de data profiling (HTML)
config/         # configuration locale (mots de passe Airflow, non versionnee)
```

## Prochaines etapes

- [ ] Creation des schemas SQL (`include/sql/`)
- [ ] Connexion PostgreSQL dans `src/db.py`
- [ ] Extraction du CSV dans `src/extract.py`
- [ ] Nettoyage et transformation dans `src/transform.py`
- [ ] Chargement en base dans `src/load.py`
- [ ] Pipeline Airflow dans `dags/`
- [ ] Rapport de profiling dans `reports/`