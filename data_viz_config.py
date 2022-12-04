import seaborn as sns
import matplotlib.pyplot as plt
from abc import ABC, abstractmethod
from generate_time import generate_time

class DataVisualization(ABC):
    @abstractmethod
    def create_figure(self, x:list[str], y:list[int]) -> plt.figure:
        pass

    @abstractmethod
    def generate_visual(self, x:list[str], y:list[int]) -> None:
        pass
    
    @abstractmethod
    def export_visual(self, x:list[str], y:list[int]) -> None:
        pass

class SeabornVisualizer(DataVisualization):
    def create_figure(self, x:list[str], y:list[int]) -> plt.figure:
        sns.set()
        figure = sns.barplot(x = x, y = y)
        return figure

    def generate_visual(self, x:list[str], y:list[int]) -> None:
        self.create_figure(x, y)
        plt.show()

    def export_visual(self, x:list[str], y:list[int]) -> None:
        self.create_figure(x, y)
        file_name = f"{generate_time()}.png"
        plt.savefig(file_name)
        return file_name

class MatplotlibVisualizer(DataVisualization):
    def create_figure(self, x:list[str], y:list[int]) -> plt.figure:
        figure = plt.bar(x = x, height = y)
        return figure

    def generate_visual(self, x:list[str], y:list[int]) -> None:
        self.create_figure(x, y)
        plt.show()

    def export_visual(self, x:list[str], y:list[int]) -> None:
        self.create_figure(x, y)
        file_name = f"{generate_time()}.png"
        plt.savefig(file_name)
        return file_name
