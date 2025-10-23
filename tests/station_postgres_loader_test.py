from meteo_jobs.load import PostgresConnector, Loader, PostgresQueriesStation
from meteo_jobs.models import Station
import pytest


records_test = [{"id_numero":38,
                 "id_nom":'38-station-meteo-toulouse-parc-jardin-des-plantes-grand-rond',
                 "longitude":1.449093882,
                 "latitude":43.5949312,
                 "altitude":144.58,
                 "emission":'N',
                 "installation":'2019-07-02',
                 "type_stati":'TH',
                 "lcz":11,
                 "ville":'Toulouse',
                 "bati":'66322.5471',
                 "veg_haute":'49491.6382',
                 "geopoint":'43.5949312, 1.449093882'}]
connector = PostgresConnector(
        host="localhost",
        port=5432,
        dbname="meteo_db_test",
        user="meteo_user",
        password="meteo_pass",
        db_queries= PostgresQueriesStation()
    )
loader = Loader(connector)


@pytest.fixture(scope="module", autouse=True)
def cleanup():
    # Code exécuté avant les tests
    print("Setup  before tests")
    yield
    # Code exécuté après tous les tests du module
    loader.close()
    print("After Tests")

def test_load_station():
    """
    it should be able to upsert a station data
    """
    station = Station(
        id_nom = "38-station-meteo-toulouse-parc-jardin-des-plantes-grand-rond",
        id_numero = 38,
        longitude = 1.449093882,
        latitude = 43.5949312,
        altitude = 144.58,
        emission = "N",
        installation = '2019-07-02',
        type_stati = "TH",
        lcz = 0.0,
        ville = "Toulouse",
        bati = "66322.5471",
        veg_haute = "49491.6382",
        geopoint = '43.5949312, 1.449093882'
    )
    stations = iter([station])
    loader.upsert_records(stations)
    records = loader.read_data()
    print(records)
    assert len(records) == 1
    loader.close()
