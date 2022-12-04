import requests
import pandas as pd
from abc import ABC, abstractmethod
from typing import Union
from collections import Counter
from data_viz_config import DataVisualization
from data_load_config import DataCloudIngestor
from generate_time import generate_time

class DataParser(ABC):
    @abstractmethod
    def clean_data(self, data:Union[dict, pd.DataFrame]) -> Union[dict, pd.DataFrame]:
        pass
    
    @abstractmethod
    def parse_data(self, data:dict) -> Union[dict, pd.DataFrame]:
        pass

    @abstractmethod
    def group_data(self, data:Union[dict, pd.DataFrame]) -> Union[dict, pd.DataFrame]:
        pass

    @abstractmethod
    def plot_data(self, data:Union[dict, pd.DataFrame], plotting_strategy: DataVisualization) -> None:
        pass

    @abstractmethod
    def export_figure(self, data:Union[dict, pd.DataFrame], plotting_strategy: DataVisualization) -> None:
        pass

class DataframeParser(DataParser):
    def clean_data(self, data:pd.DataFrame) -> pd.DataFrame:
        data.columns = data.columns.str.lower().str.replace("-", "_").str.replace(" ", "_")
        string_data = data.select_dtypes(include = "object").applymap(lambda x:x.lower() if isinstance(x, str) else x)
        data[string_data.columns] = string_data
        return data 

    def parse_data(self, data:dict) -> pd.DataFrame:
        data = pd.DataFrame(data)
        data = self.clean_data(data)
        return data

    def group_data(self, data:pd.DataFrame) -> pd.DataFrame:
        countries_grouped = data[["country", "domains"]]\
                            .groupby("country").count().reset_index()\
                            .rename(columns = {"domains":"count"})\
                            .sort_values(by = "count", ascending = False)
        return countries_grouped

    def plot_data(self, data:pd.DataFrame, plotting_strategy:DataVisualization) -> None:
        data = self.group_data(data)
        x = list(data["country"].values)
        y = list(data["count"].values)
        plotting_strategy.generate_visual(x, y)

    def export_figure(self, data:Union[dict, pd.DataFrame], plotting_strategy: DataVisualization) -> None:
        data = self.group_data(data)
        x = list(data["country"].values)
        y = list(data["count"].values)
        return plotting_strategy.export_visual(x, y)

class DictParser(DataParser):
    def clean_data(self, data:dict) -> dict:
        for r in data:
            for i in r:
                if isinstance(r[i], str):
                    r[i] = r[i].lower()
        return data

    def parse_data(self, data:dict) -> dict:
        data = [{k.lower().replace("-", "_").replace(" ", "_"):v for k, v in x.items()} for x in data]
        data = self.clean_data(data)
        return data

    def group_data(self, data:dict) -> dict:
        countries = [x["country"] for x in data]
        countries_grouped = dict(Counter(countries))
        return countries_grouped

    def plot_data(self, data:dict, plotting_strategy:DataVisualization) -> None:
        data = self.group_data(data)
        x = list(data.keys())
        y = list(data.values())
        plotting_strategy.generate_visual(x, y)

    def export_figure(self, data:Union[dict, pd.DataFrame], plotting_strategy: DataVisualization) -> None:
        data = self.group_data(data)
        x = list(data.keys())
        y = list(data.values())
        return plotting_strategy.export_visual(x, y)

class UniversitiesMgmt:
    def __init__(self, parser:DataParser):
        self.url = "http://universities.hipolabs.com/search"
        self.parser = parser

    def get_data(self, sample:int = None) -> dict:
        response = requests.get(self.url)
        if sample:
            data = response.json()[:sample]
            data = self.parser.parse_data(data)
        else:
            data = response.json()
            data = self.parser.parse_data(data)
        return data

    def plot_data(self, data:Union[dict, pd.DataFrame], plotting_strategy:DataVisualization) -> None:
        self.parser.plot_data(data, plotting_strategy)

    def export_figure(self, data:Union[dict, pd.DataFrame], plotting_strategy:DataVisualization) -> None:
        self.parser.export_figure(data, plotting_strategy)

    def ingest_information(self, data:Union[dict, pd.DataFrame], cloud_ingestor:DataCloudIngestor, plotting_strategy:DataVisualization) -> None:
        folder_name = f"{generate_time()}"
        figure_name = self.parser.export_figure(data, plotting_strategy)
        data = self.parser.group_data(data)
        data_ext = ".json" if isinstance(data, dict) else ".csv"

        cloud_ingestor.ingest_information(data, data_ext, folder_name, figure_name)





    
