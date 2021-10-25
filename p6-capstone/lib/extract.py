import logging

import pandas as pd

log = logging.getLogger(__name__)

class Extractor:
    def __init__(self, local: bool = False):
        self.base_file_path = './data' if local else 's3://p6-capstone/data'

    def get_data_location(self, file_name_or_dir: str) -> str:
        location = f"{self.base_file_path}/{file_name_or_dir}"
        log.info(f"Fetching data from: {location}")
        return location

    def immigration_data(self) -> pd.DataFrame:
        return pd.read_parquet(
            self.get_data_location("sas_data")
        )

    def temperature_data(self) -> pd.DataFrame:
        return pd.read_csv(
            self.get_data_location("GlobalLandTemperaturesByCity.csv")
        )

    def us_city_demographic_data(self) -> pd.DataFrame:
        return pd.read_csv(
            self.get_data_location("us-cities-demographics.csv"),
            sep=';'
        )

    def airport_data(self) -> pd.DataFrame:
        return pd.read_csv(
            self.get_data_location("airport-codes_csv.csv")
        )
