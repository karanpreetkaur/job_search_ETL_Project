#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-27

"""Weblog (combined log format "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"") generated via script in python

Usage: create_weblogs.py [--number_of_logs =<number_of_logs>]

Options:
--number_of_logs =<number_of_logs>  (Optional argument) The number of logs to be generated in the script [default: 10]
"""

import random
import socket
import struct
import pytz
import logging
from datetime import datetime
from docopt import docopt

opt = docopt(__doc__)

# Configure logging
logging.basicConfig(filename='weblogs.log', level=logging.INFO, format='%(message)s', filemode='w')

def create_weblogs(number_of_logs):
    # List of http status code to choose randomly from while generating logs
    http_status_code_list = [200, 201, 202, 204, 301, 302, 304, 400, 404]

    # List of user agents to choose randomly from while generating logs
    user_agent_list = ['Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1', 
    'Mozilla/5.0 (Linux; U; Android 4.4.2; en-us; SCH-I535 Build/KOT49H) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30',
    'Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 15_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) PKeyAuth/1.0',
    'Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Vivaldi/5.4.2753.47',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.27'
    ]

    for i in range(1, number_of_logs):
        # Generate random datetime between 2020 and 2022
        generate_random_login_time = datetime(random.choice([2019, 2020, 2021, 2022]), random.choice([m for m in range(1, 13) if m!=2]), random.choice(range(1, 31)), random.choice(range(24)), random.choice(range(60)), random.choice(range(60)))
        
        # Set random timezone to datetime
        timezone = pytz.timezone(random.choice(pytz.common_timezones))  

        # Add timezone to timestamp
        log_timestamp_with_zone = str(timezone.localize(generate_random_login_time).strftime("%d/%B/%Y:%H:%M:%S %z"))

        # Choose random status code from http_status_code_list for current log
        log_status_code = str(random.choice(http_status_code_list))

        # Choose random byte size for current log 
        log_bytes = str(random.choice(range(2000, 4000)))

        # Choose random user agent from user_agent_list for current log
        log_user_agent = str(random.choice(user_agent_list))

        log_usernames = random.choice([i for i in range(100, 110)])

        # Add current log to the weblogs file
        logging.info(f"{socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))} - {log_usernames} [{log_timestamp_with_zone}] \"GET /apache_pb.gif HTTP/1.0\" {log_status_code} {log_bytes} \"http://www.b2bwebsite.com/start.html\" \"{log_user_agent}\"")

def main(number_of_logs):
    if number_of_logs is None:
        number_of_logs = 10
    create_weblogs(int(number_of_logs))

if __name__ == "__main__":
    main(opt["--number_of_logs"])