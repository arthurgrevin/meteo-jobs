from meteo_jobs.models import Meteo
from typing import Iterator
from .connector import DbQueries

class PostgresQueriesMeteo(DbQueries):
    def query_create_table(self):
        return """
                CREATE TABLE IF NOT EXISTS meteo_data (
                            id_nom VARCHAR(255) PRIMARY KEY,
                            id_numero INT,
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

    def query_read_table(self):
        return "SELECT * FROM meteo_data"

    def query_upsert_records(self):
        return """
    INSERT INTO meteo_data(
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
    def get_values(self, records: Iterator[Meteo]) -> list:
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
