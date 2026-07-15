from typing import Dict 
# from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
# from basicengine.utils.funciones import ClaseEngine
from utils.funciones import ClaseEngine
from pathlib import Path


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """ 

    def __init__(self): 
        """ 
        Constructor 
        """ 
        # self.logger = get_user_logger(DataprocExperiment.__qualname__) 
    
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

if __name__ == "__main__":
    exp = DataprocExperiment()
    base_path = Path(__file__).resolve().parent.parent
    print(base_path)
    output_path = base_path / "data" / "output"
    print(output_path)
    conf = {"OUTPUT_PATH": output_path}
    exp.run(**conf)