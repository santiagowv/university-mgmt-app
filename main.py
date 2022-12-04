from app import DataframeParser, DictParser, UniversitiesMgmt
from data_viz_config import SeabornVisualizer, MatplotlibVisualizer
from data_load_config import GoogleDriveIngestor, AzureIngestor

if __name__ == "__main__":
    parser = DictParser()
    universities = UniversitiesMgmt(parser)
    data = universities.get_data(50)
    universities.plot_data(data, plotting_strategy = SeabornVisualizer())
    universities.ingest_information(data, AzureIngestor(), MatplotlibVisualizer())
