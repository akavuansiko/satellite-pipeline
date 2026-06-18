from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "angelikia",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="satellite_pipeline",
    default_args=default_args,
    description="Pipeline ETL satellites et débris spatiaux",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["satellites", "etl", "streaming"],
) as dag:

    # TASK 1 : Fetch TLE batch
    def fetch_tle(**kwargs):
        import importlib.util
        import os
        import json
        from datetime import datetime, timezone

        # Données hardcodées — pas besoin de DuckDB ici
        SATS = [
            {"norad_cat_id": 25544, "object_name": "ISS (ZARYA)", "object_type": "stations"},
            {"norad_cat_id": 48274, "object_name": "TIANHE (CSS)", "object_type": "stations"},
            {"norad_cat_id": 43013, "object_name": "STARLINK-1", "object_type": "active"},
            {"norad_cat_id": 22675, "object_name": "COSMOS 2251 DEB", "object_type": "debris"},
            {"norad_cat_id": 22677, "object_name": "IRIDIUM 33 DEB", "object_type": "debris"},
        ]

        # On écrit un fichier JSON comme staging layer
        output_path = "/opt/airflow/db/raw_satellites.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        data = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "satellites": SATS
        }
        
        with open(output_path, "w") as f:
            json.dump(data, f)
        
        print(f"ETL batch terminé — {len(SATS)} satellites écrits dans {output_path}")

    fetch_tle_task = PythonOperator(
        task_id="fetch_tle_batch",
        python_callable=fetch_tle,
    )

    # TASK 2 : Vérifier que les données sont bien là
    def check_data(**kwargs):
        import json
        import os

        output_path = "/opt/airflow/db/raw_satellites.json"
        
        if not os.path.exists(output_path):
            raise ValueError(f"Fichier {output_path} introuvable — pipeline arrêté")
        
        with open(output_path, "r") as f:
            data = json.load(f)
        
        count = len(data["satellites"])
        print(f"Nombre d'objets en base : {count}")
        
        if count == 0:
            raise ValueError("Aucune donnée chargée — pipeline arrêté")
        
        return count

    check_db = PythonOperator(
        task_id="check_data_loaded",
        python_callable=check_data,
    )

    # TASK 3 : Rafraîchir les vues analytiques
    def refresh_analytics(**kwargs):
        import json
        import os
        from collections import Counter

        input_path = "/opt/airflow/db/raw_satellites.json"
        output_path = "/opt/airflow/db/analytics.json"

        with open(input_path, "r") as f:
            data = json.load(f)

        satellites = data["satellites"]
        
        # Agrégations par type
        by_type = Counter(s["object_type"] for s in satellites)
        
        analytics = {
            "generated_at": data["fetched_at"],
            "total_objects": len(satellites),
            "by_type": dict(by_type),
            "object_names": [s["object_name"] for s in satellites]
        }

        with open(output_path, "w") as f:
            json.dump(analytics, f, indent=2)

        print(f"Vues analytiques rafraîchies : {analytics}")

    refresh_views = PythonOperator(
        task_id="refresh_analytics_views",
        python_callable=refresh_analytics,
    )

    # TASK 4 : Nettoyage
    def cleanup_old_positions(**kwargs):
        import os
        import json
        from datetime import datetime, timezone, timedelta

        positions_path = "/opt/airflow/db/positions_log.json"
        
        if not os.path.exists(positions_path):
            print("Aucun fichier de positions à nettoyer")
            return

        with open(positions_path, "r") as f:
            positions = json.load(f)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        before = len(positions)
        positions = [
            p for p in positions
            if datetime.fromisoformat(p["timestamp"]) > cutoff
        ]
        after = len(positions)

        with open(positions_path, "w") as f:
            json.dump(positions, f)

        print(f"Nettoyage effectué : {before - after} positions supprimées, {after} restantes")

    cleanup = PythonOperator(
        task_id="cleanup_old_positions",
        python_callable=cleanup_old_positions,
    )

    # Dépendances
    fetch_tle_task >> check_db >> refresh_views >> cleanup