import csv
from .extract_api import ExtractAPI
from meteo_jobs.models import Station
from typing import Iterator

class ExtractStationDataCSV(ExtractAPI):

    def __init__(self, api_url: str):
        super().__init__(api_url)
    
    def fetch_data(self, is_stream: bool, options: dict = {'delimiter': ";"}) -> Iterator:
        response = super().fetch_data(is_stream=is_stream, options = options)
        lines = (line.decode("utf-8-sig") for line in response.iter_lines() if line)
        delimiter = options['delimiter']
        return csv.DictReader(lines, delimiter = delimiter)
    

    def parse_data(self, records: Iterator) -> Iterator[Station]:
         for record in records:
            station = Station(
                id_numero = int(record["id_numero"]),
                id_nom = record["id_nom"],
                longitude = float(record["longitude"]),
                latitude =  float(record["latitude"]),
                altitude = float(record["altitude"]),
                emission = record["emission"],
                installation = record["installation"],
                type_stati = record["type_stati"],
                lcz = int(record["lcz"]),
                ville = record["ville"],
                bati = record["bati"],
                veg_haute = record["veg_haute"],
                geopoint = record["geopoint"]
            )
            yield station
