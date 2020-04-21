import argparse
import time
from datetime import datetime, timedelta
from collections import namedtuple

import utils
import logs_api
import metrica_logs_api


def get_cli_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('-date_from', help='Start of period')
    parser.add_argument('-date_to', help='End of period')
    parser.add_argument('-source', nargs='*', help='Source (hits or/and visits)')
    parser.add_argument('-gap', type=int, default=1, help='Amount of days')

    return parser.parse_args()


def write_to_file(text):
    file_object = open('log/loader.txt', 'a')
    file_object.write(text)
    file_object.close()


def get_user_request(config, date_period, source):
    start_date_str, end_date_str = date_period

    # Validate that fields are present in config
    assert '{source}_fields'.format(source=source) in config, \
        'Fields must be specified in config'
    fields = config['{source}_fields'.format(source=source)]

    # Creating data structure (immutable tuple) with initial user request
    UserRequest = namedtuple(
        "UserRequest",
        "token counter_id start_date_str end_date_str source fields"
    )

    user_request = UserRequest(
        token=config['token'],
        counter_id=config['counter_id'],
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        source=source,
        fields=tuple(fields),
    )

    utils.validate_user_request(user_request)
    return user_request


if __name__ == '__main__':
    options = get_cli_options()
    config = utils.get_config()
    metrica_logs_api.setup_logging(config)

    from_dt = datetime.strptime(options.date_from, '%Y-%m-%d')
    to_dt = datetime.strptime(options.date_to, '%Y-%m-%d')
    assert from_dt > to_dt, 'date_from must be less than date_to'
    sources = options.source
    gap = options.gap

    load = True
    start_dt = from_dt
    end_dt = from_dt - timedelta(days=(gap-1))
    while load:
        if start_dt < to_dt:
            load = False
            break

        if end_dt < to_dt:
            end_dt = to_dt
            load = False

        success = False
        for source in sources:
            start_date = start_dt.strftime('%Y-%m-%d')
            end_date = end_dt.strftime('%Y-%m-%d')

            now = datetime.now().strftime('%d-%m-%Y %H:%M')
            log = f"\n\n\n\n\n---------------------------    " \
                  f"{now} : {source}, {start_date} - {end_date}" \
                  f"    ---------------------------"
            write_to_file(log)

            try:
                user_request = get_user_request(config, (end_date, start_date), source)
                metrica_logs_api.integrate_with_logs_api(config, user_request)
            except Exception as e:
                write_to_file('Exception: ' + str(e))
            else:
                success = True

        if success:
            start_dt = end_dt - timedelta(days=1)
            end_dt = start_dt - timedelta(days=(gap-1))

        # half an hour
        time.sleep(60 * 30)
