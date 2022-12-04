from abc import ABC, abstractmethod
from azure.storage.filedatalake import DataLakeFileClient
from azure.identity import DefaultAzureCredential
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from typing import Union
import pandas as pd
import os
import json
from generate_time import generate_time

class DataCloudIngestor(ABC):
    @abstractmethod
    def connect(self, file_name:str) -> Union[DataLakeFileClient, GoogleDrive]:
        pass

    @abstractmethod
    def ingest_information(self, data:Union[dict, pd.DataFrame], data_ext:str, folder_name:str, figure_name:str) -> None:
        pass

class AzureIngestor(DataCloudIngestor):
    credentials = DefaultAzureCredential()
    
    def connect(self, file_name:str) -> DataLakeFileClient:
        file_client = DataLakeFileClient(account_url = "https://stsparkproject.dfs.core.windows.net/",
            credential = self.credentials,
            file_system_name = "university-data",
            file_path = file_name
        )
        return file_client

    def ingest_information(self, data:Union[dict, pd.DataFrame], data_ext:str, folder_name:str, figure_name:str) -> None:
        figure_path = f"{folder_name}/{figure_name}"
        figure_client = self.connect(figure_path)
        data_path = f"{folder_name}/{generate_time()}{data_ext}"
        data_client = self.connect(data_path)
        
        with open(figure_name, "rb") as file:
            file_contents = file.read()
            figure_client.upload_data(data = file_contents, overwrite = True)
        os.remove(figure_name)

        if data_ext == ".json":
            data = json.dumps(data)
            data_client.upload_data(data = data, overwrite = True)
        elif data_ext == ".csv":
            data_client.upload_data(data = data.to_csv(index = False), overwrite = True)


class GoogleDriveIngestor(DataCloudIngestor):
    def connect(self) -> GoogleDrive:
        gauth = GoogleAuth()
        drive_client = GoogleDrive(gauth)
        return drive_client

    def ingest_information(self, data:Union[dict, pd.DataFrame], data_ext:str, folder_name:str, figure_name:str) -> None:
        drive_client = self.connect()

        folder_name = folder_name
        folder_id = "parent_folder_id"
        folder = drive_client.CreateFile({"title": folder_name, "parents":[folder_id], "mimeType": "application/vnd.google-apps.folder"})
        folder.Upload()
        folder_id = folder.metadata["id"]

        data_name = f"{generate_time()}{data_ext}"
        if data_ext == ".json":
            with open(data_name, "w") as file:
                json.dump(data, file)
        elif data_ext == ".csv":
            data.to_csv(data_name, index = False)

        files = [figure_name, data_name]
        for f in files:
            file_client = drive_client.CreateFile({"parents":[{"id":folder_id}]})
            file_client.SetContentFile(f)
            file_client.Upload()

        for f in files:
            os.remove(f)



