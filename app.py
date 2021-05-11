#!/usr/bin/env python

import argparse
import asyncio
import datetime
import json
import logging
import os
import pickle
import time
from collections import defaultdict
from pathlib import Path

import coloredlogs
import pandas as pd
import requests
import yagmail

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='check cowin apis and notify registered users')
    parser.add_argument('--input-csv', type=Path,
                        default=Path("input.csv"), help='input csv file')
    parser.add_argument('--metadata-json', type=Path,
                        default=Path("metadata.json"), help='input metadata json of districts')
    parser.add_argument('--check-interval', type=float, default=5,
                        help='interval to check cowin server in mins')
    parser.add_argument('--notify-gap-interval', type=float, default=24,
                        help='gap between repeat notifications to user in hrs')
    parser.add_argument('--check-next-weeks', type=int, default=4,
                        help='check availability for next x weeks')
    parser.add_argument('--min-age-limit', type=int, default=25,
                        help='minimum age limit')
    args = parser.parse_args()

    return args


def entry(args):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        # values would be list of email ids  to notify
        districts_to_check = defaultdict(lambda: [])
        pincodes_to_check = defaultdict(lambda: [])

        # store notification history
        notif_histroy_f = Path.home() / '.cowin-notif.pickle'
        notif_history = {}

        # load notification history
        if notif_histroy_f.exists():
            with open(notif_histroy_f, "rb") as fp:
                notif_history = pickle.load(fp)

        # fetch metadata
        with open(args.metadata_json, "r") as fp:
            metadata = json.load(fp)

        # initialise email
        yag = None
        if os.getenv('GMAIL_USER') and os.getenv('GMAIL_PASSWORD'):
            yag = yagmail.SMTP(os.getenv('GMAIL_USER'),
                               os.getenv('GMAIL_PASSWORD'))
        else:
            log.warning(
                f"email sending is disabled, pass environment variables GMAIL_USER & GMAIL_PASSWORD to enable it")

        def _notify_user(email, content):
            try:
                if yag:
                    yag.send(email, 'cowin vaccine notifier', content)
                else:
                    log.warning(f"couldn't send email to {email}")
            except Exception as e:
                log.error(f"couldn't send email to {email}: {e}")

        # parse input csv
        df = pd.read_csv(args.input_csv, sep=',', header=0)
        for index, row in df.iterrows():
            (name, phone_num, email, state, dist_or_pin) = row
            phone_num = str(phone_num).lower()
            state = state.lower()
            dist_or_pin = [x.strip() for x in dist_or_pin.lower().split(';')]

            try:
                # which numbers to notify when this district/pin is ready
                for item in dist_or_pin:
                    if item.isnumeric():
                        pincodes_to_check[item].append(email)
                    else:
                        district_id = metadata[state]['districts'][item]
                        districts_to_check[district_id].append(email)
            except:
                pass

            # send registration email
            if not notif_history.get(email):
                notif_history[email] = {}
            if not notif_history[email].get('_register'):
                log.info(f'send registration email to {email}')
                _notify_user(email, [
                             'You have been registered to receive updates on availablity of cowin vaccine in your interested area.',
                             'Star and mark this email as important to receive quick notifications'])
                notif_history[email]['_register'] = True

        # generate dates strings
        date_str_lst = [(datetime.datetime.today() + datetime.timedelta(days=x*7)
                         ).strftime("%d-%m-%Y") for x in range(args.check_next_weeks)]

        def _is_available_in_sessions(sessions):
            for session in sessions:
                if session['min_age_limit'] <= args.min_age_limit and session['available_capacity'] > 0:
                    return True

            return False

        async def periodic_poll():
            while True:

                async def _check(dct, api, key1):
                    for k, v in dct.items():
                        log.debug(f'check {k}')
                        for date_s in date_str_lst:
                            res = requests.get(
                                f"https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/{api}?{key1}={k}&date={date_s}", headers=headers)

                            if res.ok:
                                res_j = json.loads(res.text)

                                availabe_at = [center['name'] for center in res_j['centers']
                                               if _is_available_in_sessions(center['sessions'])]
                                if len(availabe_at) > 0:
                                    log.debug(f"{availabe_at} notify {v}")
                                    for email in v:
                                        center_lst = []
                                        for center in availabe_at:
                                            now = time.time()
                                            if notif_history.get(email) and notif_history[email].get(center):
                                                # if last update was more than gap interval
                                                if (now - notif_history[email][center]) > (args.notify_gap_interval * 60 * 60):
                                                    center_lst.append(center)
                                                    notif_history[email][center] = now
                                            else:
                                                center_lst.append(center)
                                                if not notif_history.get(email):
                                                    notif_history[email] = {}
                                                notif_history[email][center] = now

                                        if len(center_lst) > 0:
                                            log.info(
                                                f'notify {email} of {center_lst}')
                                            _notify_user(
                                                email, ['Slots have opened in:', *center_lst])
                            else:
                                log.error(f'request failed {res}')

                await _check(pincodes_to_check, 'calendarByPin', 'pincode')
                await _check(districts_to_check, 'calendarByDistrict', 'district_id')

                await asyncio.sleep(args.check_interval * 60)

        loop = asyncio.get_event_loop()
        task = loop.create_task(periodic_poll())

        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
        finally:
            with open(notif_histroy_f, 'wb') as fp:
                pickle.dump(notif_history, fp)

            log.info('exiting ...')

    except KeyboardInterrupt:
        pass
    except Exception as e:
        log.error(e)
        raise e


if __name__ == "__main__":
    coloredlogs.install(level='DEBUG', logger=log,
                        fmt='[%(asctime)s.%(msecs)03d] - %(message)s', datefmt='%H:%M:%S')
    args = parse_args()
    entry(args)
