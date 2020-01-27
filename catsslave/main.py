import argparse
import logging
from catscore.http.request import CatsRequest
from catscore.lib.time import get_today_date
import json
from catscore.lib.logger import CatsLogging as logging
from catsslave.site.nhk import NHKSite

def main():
    parser = argparse.ArgumentParser(description="cats slave")

    # args params
    parser.add_argument('-conf', '--conf', help="configuration file", required=True)
    parser.add_argument('-nhk', '--nhk', nargs='*', choices=['program_list'], help="nhk functions")
    args = parser.parse_args()
    print(args)
    
    # init
    with open(args.conf) as f:
        conf = json.load(f)
        print(conf)
        try:
            logging.init(app_name=conf["logging"]["app_name"], ouput_dir=conf["logging"]["log_dir"], log_level=conf["logging"]["log_level"])
        except Exception:
            print("logging conf not found.")

    if args.nhk:
        for args in args.nhk:
            NHKSite.save_program_list_as_csv(output_dir=conf["output"]["dir"], api_key=conf["nhk"]["api_key"])

if __name__ == "__main__":
    main()
