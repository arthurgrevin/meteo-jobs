from .connector import Connector
from typing import Iterator

class Loader:

    def __init__(self, connector: Connector):
        self.connector = connector

    def upsert_records(self, records: Iterator):
        return self.connector.upsert_records(records)

    def read_data(self):
        return self.connector.read_table()

    def close(self):
        return self.connector.close()
