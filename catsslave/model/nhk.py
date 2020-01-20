from dataclasses import dataclass, field
from typing import List
from catscore.lib.time import get_today_date

@dataclass(frozen=True)
class NHKProgram:
    id: int
    event_id: str
    start_time: str
    end_time: str
    area_id: str
    area_name: str
    service_id: str
    service_name: str
    title: str
    subtitle: str
    content: str
    act: str
    genres: str
    update_date:str

class NHKProgramTable:
    dir_name = "nhk"
    table_name = "nhk_program"
    
    @classmethod
    def csv_path(cls, output_dir):
        return f"{output_dir}/{cls.dir_name}/{cls.table_name}_{get_today_date()}.csv"
    
    @classmethod
    def parse_response(cls, response_json: str) -> List[NHKProgram]:
        """[summary]
        
        Arguments:
            response_json {str} -- [description]
        
        Returns:
            List[NHKProgram] -- [description]
        """
        def _parse(j) -> NHKProgram:
            return NHKProgram(
                id=j["id"],
                event_id=j["event_id"],
                start_time=j["start_time"],
                end_time=j["end_time"],
                area_id=j["area"]["id"],
                area_name=j["area"]["name"],
                service_id=j["service"]["id"],
                service_name=j["service"]["name"],
                title=j["title"],
                subtitle=j["subtitle"],
                content=j["content"],
                act=j["act"],
                genres=",".join(j["genres"]),
                update_date=get_today_date())
        return list(map(lambda j: _parse(j), response_json["list"]["g1"]))