from meteo_jobs.extract import ExtractMeteoDataCSV
from meteo_jobs.load import MeteoPostgresLoader
import os

print("Start Extract and Load Meteo Data to Postgres")

if __name__ == "__main__":

    STATION = os.getenv("STATION")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWD = os.getenv("DB_PASSWD")
    api_url =  f"https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/{STATION}/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    extract = ExtractMeteoDataCSV(api_url)
    connector = MeteoPostgresLoader(
        host = DB_HOST,
        port = DB_PORT,
        dbname = DB_NAME,
        user = DB_USER,
        password = DB_PASSWD

    )
    records = extract.fetch_data()
    meteos = extract.parse_data(records)
    connector.upsert_records(records)
    connector.close()

    print("End of Extract and Load")
