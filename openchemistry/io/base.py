from abc import ABC, abstractmethod

class BaseReader(ABC):

    @staticmethod
    @abstractmethod
    def read(f):
        """
        Converts an output file to CJSON

        Parameters
        ----------
        f : file-like object
            A quantum chemistry output file

        Returns
        -------
        cjson : dict
            A cjson compliant dict
        """
        pass
