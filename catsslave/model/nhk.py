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
    
        @classmethod
    
    @classmethod
    def to_db(cls, spark:SparkSession, df:DataFrame, mysql_conf:MySQLConf):
        df.write.jdbc(mysql_conf.connection_uri("jdbc"), table=cls._table_name, mode='overwrite')

    @classmethod
    def cooking(cls, spark:SparkSession, df:DataFrame, mecab_dict: str):
        def _cooking(d):
            s = f'{d["content"]}'
            mecab = CatsMeCab(mecab_dict)
            parsed_s = mecab.parse(str(s))
            noun_s = list(filter(lambda r: r.word_type == "名詞", parsed_s))
            noun_one_str = list(map(lambda r: f"{r.word}", noun_s))
            nouns = ",".join(noun_one_str)
            result = CookedNHKProgram(
                id = d["id"],
                event_id = d["event_id"],
                start_time = d["start_time"],
                end_time = d["end_time"],
                area_id = d["area_id"],
                area_name = d["area_name"],
                service_id = d["service_id"],
                service_name = d["service_name"],
                title = d["title"],
                subtitle = d["subtitle"],
                content = d["content"],
                act = d["act"],
                genres = d["genres"],
                update_date = d["update_date"],
                nouns = nouns)
            return result
        return df.rdd.map(lambda d: _cooking(d)).toDF()

@dataclass(frozen=True)
class CookedNHKProgram:
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
    update_date: str
    nouns: str

class CookedNHKProgramTable:
    _table_name = "cooked_nhk_program"

    @classmethod
    def to_db(cls, spark:SparkSession, df:DataFrame, mysql_conf:MySQLConf):
        df.write.jdbc(mysql_conf.connection_uri("jdbc"), table=cls._table_name, mode='overwrite')