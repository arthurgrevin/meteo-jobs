from .extract_api import ExtractAPI
import csv
from typing import Iterator
from meteo_jobs.models import Meteo

class ExtractMeteoDataCSV(ExtractAPI):

    def __init__(self, api_url: str):
        super().__init__(api_url)


    def fetch_data(self, is_stream: bool, options: dict = {'delimiter': ";"}) -> Iterator:
        response = super().fetch_data(is_stream=is_stream, options = options)
        lines = (line.decode("utf-8-sig") for line in response.iter_lines() if line)
        delimiter = options['delimiter']
        return csv.DictReader(lines, delimiter = delimiter)


    def parse_data(self, records:Iterator) -> Iterator:
        for record in records:
            meteo = Meteo(
                data = record["data"],
                id = int(record["id"]),
                humidite = int(record["humidite"]),
                direction_du_vecteur_de_vent_max =
                    int(record["direction_du_vecteur_de_vent_max"]),
                pluie_intensite_max = float(record["pluie_intensite_max"]),
                pression = int(record["pression"]),
                direction_du_vecteur_vent_moyen =
                    int(record["direction_du_vecteur_vent_moyen"]),
                type_de_station = record["type_de_station"],
                pluie = float(record["pluie"]),
                direction_du_vecteur_de_rafale_de_vent_max =
                float(record["direction_du_vecteur_de_rafale_de_vent_max"]),
                force_moyenne_du_vecteur_vent =
                    int(record["force_moyenne_du_vecteur_vent"]),
                force_rafale_max = int(record["force_rafale_max"]),
                temperature = float(record["temperature"]),
                heure_de_paris = record["heure_de_paris"],
                heure_utc = record["heure_utc"]
            )
            yield meteo
