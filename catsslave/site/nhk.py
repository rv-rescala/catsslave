from catscore.http.request import CatsRequest
from catscore.lib.time import get_today_date
from catscore.lib.logger import CatsLogging as logging
from catsslave.model.nhk import NHKProgramTable, NHKProgram
import sys
from typing import List
from catscore.lib.pandas import PandasConverter

class NHKSite:
    def __init__(self):
        """[summary]
        
        Arguments:
            api_key {str} -- [description]
        """

    @classmethod
    def program_list(cls, api_key, area="130", service="g1", date=None) -> List[NHKProgram]:
        request = CatsRequest()
        with CatsRequest() as request:
            if date == None:
                date = get_today_date(split="-")
            try:
                api_url = f"http://api.nhk.or.jp/v2/pg/list/130/g1/{date}.json?key={api_key}"
                logging.info(f"api_url: {api_url}",sys._getframe().f_code.co_name)
                json = request.get(url=api_url, response_content_type="json").content
                logging.debug(f"response: {json}",sys._getframe().f_code.co_name)
                result = NHKProgramTable.parse_response(response_json=json)
                return result
            except Exception:
                sys.exc_info()
                logging.error(f"{sys._getframe().f_code.co_name} was failed. {sys.exc_info()}")

    @classmethod
    def save_program_list_as_csv(cls, output_dir: str, api_key, area="130", service="g1", date=None):
        result = cls.program_list(api_key, area, service, date)
        df = PandasConverter.dataclass_to_dataframe(result)
        csv_path = NHKProgramTable.csv_path(output_dir=output_dir)
        logging.info(f"{sys._getframe().f_code.co_name} output to {csv_path}")
        df.to_csv(csv_path)
