import requests


class Extract:

    def __init__(self, logger):
        self.logger = logger
        self.URL = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100"
                    "&page=1&sparkline=false&locale=en")
        self.data = None

    def get_coins_data(self):
        # get data from coingecko API. Only url needed
        try:
            self.logger.info(f"Fetching data from {self.URL}")
            response = requests.get(self.URL)
            json_response = response.json()
            self.data = json_response
        except Exception as e:
            print(f"API fetch failed. Error: {e}")
            self.logger.error(f"API data fetch failed. Error: {e}")
