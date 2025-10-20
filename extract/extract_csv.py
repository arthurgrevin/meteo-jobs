from .extract_api import ExtractAPI
import csv
from models import Meteo

class ExtractMeteoDataCSV(ExtractAPI):

    def __init__(self, api_url):
        super().__init__(api_url)


    def fetch_data(self, delimiter, ):
        response = super().fetch_data(is_stream=True)
        lines = (line.decode("utf-8-sig") for line in response.iter_lines() if line)

        return csv.DictReader(lines, delimiter = delimiter)


    def parse_data(self, records):
        for record in records:
            meteo = Meteo(
                data = record["data"],
                id = record["id"],
                humidite = record["humidite"],
                direction_du_vecteur_de_vent_max =
                    record["direction_du_vecteur_de_vent_max"],
                pluie_intensite_max = record["pluie_intensite_max"],
                pression = record["pression"],
                direction_du_vecteur_vent_moyen =
                    record["direction_du_vecteur_vent_moyen"],
                type_de_station = record["type_de_station"],
                pluie = record["pluie"],
                direction_du_vecteur_de_rafale_de_vent_max =
                    record["direction_du_vecteur_de_rafale_de_vent_max"],
                force_moyenne_du_vecteur_vent = record["force_moyenne_du_vecteur_vent"],
                force_rafale_max = record["force_rafale_max"],
                temperature = record["temperature"],
                heure_de_paris = record["heure_de_paris"],
                heure_utc = record["heure_utc"]
            )
            yield meteo
