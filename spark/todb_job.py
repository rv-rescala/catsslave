from __future__ import print_function

import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from catscore.db.mysql import MySQLConf
from catslab.word.mecab import CatsMeCab
from catsslave.model.nhk import NHKProgramTable, CookedNHKProgramTable

def main():
    # spark
    conf = SparkConf()
    conf.setAppName('relux')
    sc: SparkContext = SparkContext(conf=conf)
    spark:SparkSession = SparkSession(sc)

    # init
    parser = argparse.ArgumentParser(description='pyspark app args')
    parser.add_argument('-ip', '--input_path', type=str, required=True, help='input folder path')
    parser.add_argument('-db', '--db_conf', type=str, required=True, help='input db conf path')
    parser.add_argument('-mc', '--mecab_conf', type=str, required=True, help='input db conf path')
    args = parser.parse_args()
    print(f"args: {args}")
    ## db
    mysql_conf = MySQLConf.from_json(args.db_conf)
    print(f"mysql_conf {mysql_conf}")
    ## mecab
    mecab = CatsMeCab.from_json(args.mecab_conf)
    
    # convert
    # step1
    ranked_item_df = NHKProgramTable.from_file_as_df(spark, args.input_path)
    NHKProgramTable.to_db(spark=spark, df=ranked_item_df, mysql_conf=mysql_conf)
    
    # step2
    cooked_ranked_item = NHKProgramTable.cooking(spark=spark, df=ranked_item_df, mecab_dict=mecab.dict_path)
    CookedNHKProgramTable.to_db(spark=spark, df=cooked_ranked_item, mysql_conf=mysql_conf)
    
if __name__ == '__main__':
    main()