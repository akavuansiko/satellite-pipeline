from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

default_args = {
    "owner": "angelikia",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="satellite_pipeline",
    default_args=default_args,
    description="Pipeline ETL satellites et débris spatiaux",
    schedule_interval="0 */2 * * *",  # toutes les 2h (respecte limite CelesTrak)
    start_date=datetime(2026, 6, 16),
    catchup=False,
    tags=["satellites", "etl", "streaming"],
) as dag:

    # TASK 1 : Fetch TLE batch depuis CelesTrak
    fetch_tle = BashOperator(
        task_id="fetch_tle_batch",
        bash_command="cd /opt/airflow && python /opt/airflow/etl/fetch_tle.py",
    )

    # TASK 2 : Vérifier que les données sont bien en base
    def check_data(**kwargs):
        import duckdb
        DB_PATH = "/opt/airflow/db/satellites.duckdb"
        con = duckdb.connect(DB_PATH)
        result = con.execute("SELECT COUNT(*) FROM raw_satellites").fetchone()
        count = result[0]
        print(f"Nombre d'objets en base : {count}")
        if count == 0:
            raise ValueError("Aucune donnée chargée — pipeline arrêté")
        con.close()
        return count

    check_db = PythonOperator(
        task_id="check_data_loaded",
        python_callable=check_data,
    )

    # TASK 3 : Rafraîchir les vues analytiques ELT
    def refresh_analytics(**kwargs):
        import duckdb
        DB_PATH = "/opt/airflow/db/satellites.duckdb"
        con = duckdb.connect(DB_PATH)

        # Vue par type d'objet
        con.execute("""
            CREATE OR REPLACE VIEW analytics_by_type AS
            SELECT
                object_type,
                COUNT(*)            AS total,
                AVG(inclination)    AS avg_inclination,
                AVG(mean_motion)    AS avg_mean_motion,
                fetched_at::DATE    AS fetch_date
            FROM raw_satellites
            GROUP BY object_type, fetch_date
        """)

        # Vue orbites basses (LEO < 2000 km)
        con.execute("""
            CREATE OR REPLACE VIEW analytics_leo AS
            SELECT
                object_name,
                norad_cat_id,
                inclination,
                mean_motion,
                eccentricity,
                fetched_at
            FROM raw_satellites
            WHERE mean_motion > 11.25  -- LEO : > 11.25 révolutions/jour
            ORDER BY mean_motion DESC
        """)

        # Vue anomalies : excentricité élevée (orbites elliptiques)
        con.execute("""
            CREATE OR REPLACE VIEW analytics_anomalies AS
            SELECT
                object_name,
                norad_cat_id,
                object_type,
                eccentricity,
                inclination,
                fetched_at
            FROM raw_satellites
            WHERE eccentricity > 0.1
            ORDER BY eccentricity DESC
        """)

        print("Vues analytiques rafraîchies avec succès")
        con.close()

    refresh_views = PythonOperator(
        task_id="refresh_analytics_views",
        python_callable=refresh_analytics,
    )

    # TASK 4 : Nettoyage des vieilles positions (garder 24h)
    def cleanup_old_positions(**kwargs):
        import duckdb
        DB_PATH = "/opt/airflow/db/satellites.duckdb"
        con = duckdb.connect(DB_PATH)
        con.execute("""
            DELETE FROM satellite_positions
            WHERE timestamp < NOW() - INTERVAL '24 hours'
        """)
        remaining = con.execute("SELECT COUNT(*) FROM satellite_positions").fetchone()[0]
        print(f"Nettoyage effectué. Positions restantes : {remaining}")
        con.close()

    cleanup = PythonOperator(
        task_id="cleanup_old_positions",
        python_callable=cleanup_old_positions,
    )

    # Dépendances des tasks
    fetch_tle >> check_db >> refresh_views >> cleanup