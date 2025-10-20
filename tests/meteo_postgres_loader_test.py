from load import MeteoPostgresLoader



records_test = [{'data': '0195236c9af000002c882c00',
               'id': 0,
               'humidite': 94,
               'direction_du_vecteur_de_vent_max': 4,
               'pluie_intensite_max': 0.0,
               'pression': 100000,
               'direction_du_vecteur_vent_moyen': 0,
               'type_de_station': 'ISS',
               'pluie': 0.0,
               'direction_du_vecteur_de_vent_max_en_degres': 90.0,
               'force_moyenne_du_vecteur_vent': 1,
               'force_rafale_max': 11,
               'temperature_en_degre_c': 0.6,
               'heure_de_paris': '2021-12-21T06:30:00+00:00',
               'heure_utc': '2021-12-21T06:30:00+00:00'}]

loader = MeteoPostgresLoader(
        host="localhost",
        port=5432,
        dbname="meteo_db_test",
        user="meteo_user",
        password="meteo_pass"
    )


def test_load_record():
    """
    It should be able to upsert record to postgres
    """
    loader.upsert_records(records_test)
    records = loader.read_meteo_table()
    print(records)
    assert len(records) == 1
    loader.close()







