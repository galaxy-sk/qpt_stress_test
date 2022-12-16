
from abc import ABC, abstractmethod

class SqlQueryInterface(ABC):

    @abstractmethod
    def as_dataframe(self):
        pass

    @abstractmethod
    def as_map(self) -> dict:
        pass

    @abstractmethod
    def as_list(self) -> tuple:
        pass
