from typing import Dict 
import logging
from basicengine.utils.funciones import ClaseEngine


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """ 

    def __init__(self): 
        """ 
        Constructor 
        """ 
        self.logger = logging.getLogger(DataprocExperiment.__qualname__) 
    
    def run(self, **parameters: Dict) -> None: 
        """ 
        Execute the code written by the user. 
    
        Args: 
            parameters: The config file parameters 
        """ 
    
        #  Parameters 
        OUTPUT_PATH = str(parameters["OUTPUT_PATH"])

        try:
            engine = ClaseEngine(OUTPUT_PATH)
            engine.run()
        except Exception as e:
            print(f"ERROR: {e}")
        finally:
            print('Final proceso')
