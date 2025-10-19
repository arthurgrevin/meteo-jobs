import psycopg2
from psycopg2.extras import execute_values

class MeteoPostgresLoader:
    """
    PostGresLoad to connect then upsert records to meteo_data table
    """

    def __init__(self, host, port, dbname, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self.conn.autocommit = True
        print("Connected to PostgreSQL")
        self._create_table()

    def _create_table(self):
        """Create table is does not exist"""
        query = """
        CREATE TABLE IF NOT EXISTS meteo_data (
            id SERIAL PRIMARY KEY,
            data_id VARCHAR(255),
            direction_du_vecteur_de_vent_max INT,
            pluie_intensite_max FLOAT,
            type_de_station VARCHAR(255),
            direction_du_vecteur_de_vent_max_en_degres FLOAT,
            force_moyenne_du_vecteur_vent INT,
            force_rafale_max INT,
            temperature_en_degre_c FLOAT,
            humidite INT,
            pression INT,
            pluie FLOAT,
            heure_utc TIMESTAMP,
            heure_de_paris TIMESTAMP,
            direction_du_vecteur_vent_moyen INT,
            UNIQUE (data_id, heure_utc)
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(query)

    def upsert_records(self, records):
        """
        Upsert records
        :param records: list[dict]
        """
        if not records:
            print("No records to upsert")
            return

        query = """
        INSERT INTO meteo_data (
            data_id,
            direction_du_vecteur_de_vent_max,
            pluie_intensite_max,
            type_de_station,
            direction_du_vecteur_de_vent_max_en_degres,
            force_moyenne_du_vecteur_vent,
            force_rafale_max,
            temperature_en_degre_c,
            humidite,
            pression,
            pluie,
            heure_utc,
            heure_de_paris,
            direction_du_vecteur_vent_moyen
        )
        VALUES %s
        ON CONFLICT (data_id, heure_utc) DO NOTHING
        """

        values = [
            (
                r.get("data"),
                r.get("direction_du_vecteur_de_vent_max"),
                r.get("pluie_intensite_max"),
                r.get("type_de_station"),
                r.get("direction_du_vecteur_de_vent_max_en_degres"),
                r.get("force_moyenne_du_vecteur_vent"),
                r.get("force_rafale_max"),
                r.get("temperature_en_degre_c"),
                r.get("humidite"),
                r.get("pression"),
                r.get("pluie"),
                r.get("heure_utc"),
                r.get("heure_de_paris"),
                r.get("direction_du_vecteur_vent_moyen"),
            )
            for r in records
        ]

        print(values)

        with self.conn.cursor() as cur:
            execute_values(cur, query, values)
        print(f"{len(records)} records upsert in PostgreSQL")


    def read_meteo_table(self):
        """
        Read meteo from postgres table
        """
        query = "SELECT * FROM meteo_data"
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return rows


    def close(self):
        """Close Database connection"""
        if self.conn:
            self.conn.close()
            print("Connexion PostgreSQL closed")
