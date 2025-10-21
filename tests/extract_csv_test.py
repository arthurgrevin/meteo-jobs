from meteo_jobs.extract import ExtractMeteoDataCSV



API_URL = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/00-station-meteo-toulouse-valade/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
DELIMITER = ";"
extract = ExtractMeteoDataCSV(API_URL)


records = extract.fetch_data(DELIMITER)

def test_extract_csv():
    """
    it should be able to download the csv and return an Iterator of record
    """
    record = next(records)
    print(record)
    assert record is not None
    assert record['data'] is not None
    assert record['id'] is not None

def test_parse_csv():
    "it should be able to parse into a Meteo model"
    print(next(records))
    meteos = extract.parse_data(records)
    meteo = next(meteos)
    print(meteo)
    assert meteo is not None
    assert meteo.data is not None
    assert meteo.id is not None
