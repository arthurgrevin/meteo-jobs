import psycopg2
from psycopg2.extras import execute_values
from itertools import islice
from meteo_jobs.models import Meteo
from typing import Iterator


QUERY_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS meteo_data (
                data_id VARCHAR(255) PRIMARY KEY,
                id INT,
                direction_du_vecteur_de_vent_max INT,
                pluie_intensite_max FLOAT,
                type_de_station VARCHAR(255),
                direction_du_vecteur_de_rafale_de_vent_max FLOAT,
                force_moyenne_du_vecteur_vent INT,
                force_rafale_max INT,
                temperature FLOAT,
                humidite INT,
                pression INT,
                pluie FLOAT,
                heure_utc VARCHAR(255),
                heure_de_paris VARCHAR(255),
                direction_du_vecteur_vent_moyen INT,
                UNIQUE (data_id)
            );
"""

QUERY_UPSERT_RECORDS = """
INSERT INTO meteo_data (
            data_id,
            direction_du_vecteur_de_vent_max,
            pluie_intensite_max,
            type_de_station,
            direction_du_vecteur_de_rafale_de_vent_max,
            force_moyenne_du_vecteur_vent,
            force_rafale_max,
            temperature,
            humidite,
            pression,
            pluie,
            heure_utc,
            heure_de_paris,
            direction_du_vecteur_vent_moyen
        )
        VALUES %s
        ON CONFLICT (data_id) DO NOTHING
"""

QUERY_READ_TABLE = "SELECT * FROM meteo_data"



class MeteoPostgresLoader:
    """
    PostGresLoad to connect then upsert records to meteo_data table
    """

    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.conn.autocommit = True
        print("Connected to PostgreSQL")
        self._create_table()

    def _get_values(self, records: Iterator[Meteo]) -> list:
        values = [
            (
                r.get("data"),
                r.get("direction_du_vecteur_de_vent_max"),
                r.get("pluie_intensite_max"),
                r.get("type_de_station"),
                r.get("direction_du_vecteur_de_rafale_de_vent_max"),
                r.get("force_moyenne_du_vecteur_vent"),
                r.get("force_rafale_max"),
                r.get("temperature"),
                r.get("humidite"),
                r.get("pression"),
                r.get("pluie"),
                r.get("heure_utc"),
                r.get("heure_de_paris"),
                r.get("direction_du_vecteur_vent_moyen"),
            )
            for r in records
        ]
        return values

    def _create_table(self) -> None:
        """Create table is does not exist"""
        with self.conn.cursor() as cur:
            cur.execute(QUERY_CREATE_TABLE)

    def upsert_records(self, records, batch_size=10000) -> None:
        """
        Upsert records
        :param records: iterator[dict]
        """
        if not records:
            print("No records to upsert")
            return
        while True:
            batch = list(islice(records, batch_size))
            if not batch:
                break
            values = self._get_values(batch)

            with self.conn.cursor() as cur:
                execute_values(cur, QUERY_UPSERT_RECORDS, values)
            print(f"{len(batch)} records upsert in PostgreSQL")
        print("End of upsert")


    def read_meteo_table(self) -> list:
        """
        Read meteo from postgres table
        """
        with self.conn.cursor() as cur:
            cur.execute(QUERY_READ_TABLE)
            rows = cur.fetchall()
        return rows


    def close(self) -> None:
        """Close Database connection"""
        if self.conn:
            self.conn.close()
            print("Connexion PostgreSQL closed")
