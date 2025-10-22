import requests
from typing import Protocol, Iterator

class ExtractAPI(Protocol):

    def __init__(self, api_url: str):
        self.api_url = api_url


    def fetch_data(self, is_stream: bool, options: dict) -> Iterator:
        """
            fetch data using api_url
        """
        response = requests.get(self.api_url, stream=is_stream)
        response.raise_for_status()
        return response



    def parse_data(self, records: Iterator) -> Iterator:
        """
            parse data to a model
        """
