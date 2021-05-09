#!/usr/bin/env python

import json
import logging

import requests


def generate_metadata(dump_to='metadata.json'):
    try:
        # fetch info
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        res = requests.get(
            "https://cdn-api.co-vin.in/api/v2/admin/location/states", headers=headers)
        res_j = json.loads(res.text)

        meta = {}
        for state in res_j['states']:
            state_name = state["state_name"].lower()
            state_id = state["state_id"]

            meta[state_name] = {
                'state_id': state_id,
                'districts': {},
            }

            logging.info(f"querying districts in {state_name}")
            res = requests.get(
                f"https://cdn-api.co-vin.in/api/v2/admin/location/districts/{state_id}", headers=headers)
            res_j = json.loads(res.text)

            for district in res_j['districts']:
                district_name = district["district_name"].lower()
                district_id = district["district_id"]

                meta[state_name]['districts'][district_name] = district_id

        if dump_to:
            with open(dump_to, 'w') as fp:
                json.dump(meta, fp, indent=2)

        return meta

    except Exception as e:
        logging.error(e)
        raise e

if __name__ == "__main__":
    generate_metadata()
