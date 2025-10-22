from meteo_jobs.extract import Extract, ExtractMeteoDataCSV
from meteo_jobs.load import Loader, PostGresConnectorMeteo
import os
import argparse


print("Start Extract and Load Meteo Data to Postgres")

if __name__ == "__main__":

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWD = os.getenv("DB_PASSWD")

    parser = argparse.ArgumentParser(description="Extract and Load Meteo")
    parser.add_argument(
        "--station",
        type=str,
        default="00-station-meteo-toulouse-valade",
        help="enter station name"
    )

    args = parser.parse_args()  # parse les arguments
    if args.station:
        station = args.station

    api_url =  f"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/{station}/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    extract = Extract(ExtractMeteoDataCSV(api_url))
    loader = Loader(PostGresConnectorMeteo(
        host = DB_HOST,
        port = DB_PORT,
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASSWD

    ))
    records = extract.fetch_data(options = {'delimiter':";"})
    meteos = extract.parse_data(records)
    loader.upsert_records(records)
    loader.close()

    print("End of Extract and Load")
