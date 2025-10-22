from meteo_jobs.extract import Extract, ExtractStationDataCSV
from meteo_jobs.models import Station
from meteo_jobs.load import MeteoPostgresLoader
from meteo_jobs.action import ActionExecutor, ActionStation
from typing import Iterator
import os

print("Start Extract and Load Meteo Data to Postgres")


def extract_stations(extract: Extract) -> Iterator[Station]:
    options ={'delimiter': ';'}
    records  = extract.fetch_data(options=options)
    return extract.parse_data(records)

def launch_process(action_executor:ActionExecutor, stations: Iterator[Station]) -> Iterator[Station]:
    return action_executor.execute(stations)

def load_station(connector: MeteoPostgresLoader,
                 stations : Iterator[Station]):
    for station in stations:
        print(f"save station {station.id_nom}")
    

if __name__ == "__main__":

    API_ID = os.getenv("API_ID")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWD = os.getenv("DB_PASSWD")
    api_url =  f"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/{API_ID}/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    extract = Extract(ExtractStationDataCSV(api_url))
    connector = MeteoPostgresLoader(
        host = DB_HOST,
        port = DB_PORT,
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASSWD

    )
    action_executor = ActionExecutor(ActionStation())
    stations = launch_process(action_executor, extract_stations(extract))
    load_station(connector, stations)
    
    connector.close()

    print("End of Extract and Load")