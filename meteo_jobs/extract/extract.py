from .extract_api import ExtractAPI
from typing import Iterator

class Extract:

    def __init__(self, extract_impl: ExtractAPI):
        self.extract_impl = extract_impl


    def fetch_data(self, is_stream: bool = True, options: dict = {}) -> Iterator:
        """Fetch data using ExtractAPI implementation"""
        return self.extract_impl.fetch_data(is_stream, options)

    def parse_data(self, records: Iterator) -> Iterator:
        """Parse Data using ExtractAPI implementation"""
        return self.extract_impl.parse_data(records)
