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
        return "SELECT * FROM station ORDER BY id_numero"

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
                    r.id_numero,
                    r.id_nom,
                    r.longitude,
                    r.latitude,
                    r.altitude,
                    r.emission,
                    r.installation,
                    r.type_stati,
                    r.lcz,
                    r.ville,
                    r.bati,
                    r.veg_haute,
                    r.geopoint
                )
                for r in records
            ]
            return values

    def parse_data(self, result)->list[Station]:
         stations = [
              (
                   Station(
                        r[0],
                        r[1],
                        r[2],
                        r[3],
                        r[4],
                        r[5],
                        r[6],
                        r[7],
                        r[8],
                        r[9],
                        r[10],
                        r[11],
                        r[12])
              )

              for r in result
         ]
         return stations
