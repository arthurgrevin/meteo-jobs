from meteo_jobs.models import Station
from typing import Iterator
from .connector import DbQueries

class PostgresQueriesStation(DbQueries):
    def query_create_table(self):
        return """
                CREATE TABLE IF NOT EXISTS station (
                            id_numero SERIAL PRIMARY KEY,
                            id_nom VARCHAR(255),
                            longitude FLOAT,
                            latitude FLOAT,
                            altitude FLOAT,
                            emission VARCHAR(255),
                            installation VARCHAR(255),
                            type_stati VARCHAR(255),
                            lcz int,
                            ville VARCHAR(255),
                            bati VARCHAR(255),
                            veg_haute VARCHAR(255),
                            geopoint VARCHAR(255),
                            UNIQUE (id_numero)
                        );
                """

    def query_read_table(self):
        return "SELECT * FROM station"

    def query_upsert_records(self):
        return """
    INSERT INTO station(
            id_numero,
            id_nom,
            longitude,
            latitude,
            altitude,
            emission,
            installation,
            type_stati,
            lcz,
            ville,
            bati,
            veg_haute,
            geopoint
        )
        VALUES %s
        ON CONFLICT (id_numero) DO NOTHING
"""

    def get_values(self, records: Iterator[Station]) -> list:
            values = [
                (
                    r.get("id_numero"),
                    r.get("id_nom"),
                    r.get("longitude"),
                    r.get("latitude"),
                    r.get("altitude"),
                    r.get("emission"),
                    r.get("installation"),
                    r.get("type_stati"),
                    r.get("lcz"),
                    r.get("ville"),
                    r.get("bati"),
                    r.get("veg_haute"),
                    r.get("geopoint")
                )
                for r in records
            ]
            return values
