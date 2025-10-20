import requests


class ExtractAPI:

    def __init__(self, api_url):
        self.api_url = api_url


    def fetch_data(self, is_stream):
        """
            fetch data using api_url
        """
        response = requests.get(self.api_url, stream=is_stream)
        response.raise_for_status()
        return response



    def parse_data(self):
        """
            parse data to a model
        """
