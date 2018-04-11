#!/usr/bin/env python3
# index_rebuilder.py - the utility rebuilds
# PostgreSQL indexes without table locking and shows related index statistics
#
# Author: Andrey Klychkov aaklychkov@mail.ru
# Date: 10-04-2018
# See https://github.com/Andersson007/index-rebuilder for more info
#
# Requirements: Python3+, psycopg2, pyyaml

import argparse
import datetime
import logging
import os
import shutil
import socket
import subprocess
import sys

import lib.database as db
from lib.common import ConfParser, Mail

#=======================
#   Parameters block   #
#=======================

# Mail report massage:
report_list = []

# Common params:
__VERSION__ = '2.2.1'
HOSTNAME = socket.gethostname()
TODAY = datetime.date.today().strftime('%Y%m%d')


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="rebuild indexes and show related index statistic")

    parser.add_argument("-c", "--config", dest="config", required=True,
                        help="path to configuration FILE", metavar="FILE",
                        default=False)
    parser.add_argument("-d", "--datbase", dest="dbname", default=False,
                        help="database name", required=True)
    parser.add_argument("-p", "--port", dest="db_port",
                        help="database port", metavar="PORT")
    parser.add_argument("-H", "--host", dest="db_host",
                        help="database host", metavar="HOST")
    parser.add_argument("-U", "--user", dest="db_user",
                        help="database user", metavar="USER")
    parser.add_argument("-P", "--passwd", dest="db_passwd",
                        help="db user password", metavar="PASSWD")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--stat", action="store_true",
                       help="show top of bloated indexes")
    group.add_argument("-u", "--unused", dest="scan_counter",
                       type=int, default=None,
                       help="show unused indexes with SCAN_COUNTER")
    group.add_argument("-i", "--invalid", action="store_true",
                       help="show invalid indexes")
    group.add_argument("-r", "--rebuild", dest="index", default=False,
                       help="rebuild a specified index")
    group.add_argument("-f", "--file", dest="filename", default=False,
                       help="rebuild indexes from FILE", metavar="FILE")
    group.add_argument("--version", action="version",
                       version=__VERSION__, help="show version and exit")

    return parser.parse_args()

args = parse_cli_args()

# ======================================
# Parsing of configuration files section
# ======================================

# Allowable parameters list in a config file:
params = ['lock_query_timeo',
          'log_dir',
          'log_pref',
          'mail_allow',
          'mail_subject',
          'smtp_acc',
          'mail_recipient',
          'smtp_srv',
          'smtp_port',
          'smtp_pass',
          'mail_sender']

conf_parser = ConfParser()
conf_parser.set_params(params)
conf_parser.set_config(args.config)
configuration = conf_parser.get_options()

# Main params:
# Statement timeout (in this util for drop/alter sql only):
LOCK_QUERY_TIMEO = '0'
if configuration['lock_query_timeo']:
    LOCK_QUERY_TIMEO = configuration['lock_query_timeo']

# Log params:
LOG_DIR = configuration['log_dir']
LOG_PREF = configuration['log_pref']

# Mail params:
ALLOW_MAIL_NOTIFICATION = int(configuration['mail_allow'])
SBJ = configuration['mail_subject']
SMTP_ACC = configuration['smtp_acc']
RECIPIENT = configuration['mail_recipient']
SMTP_SRV = configuration['smtp_srv']
SMTP_PORT = configuration['smtp_port']
SMTP_PASS = configuration['smtp_pass']
SENDER = configuration['mail_sender']

# DB defaults below.
# In the DatBase.get_connect() class method
# a database name for connection is 'postgres' by default):
DB_CONTYPE = 'u_socket'
DB_USER = 'postgres'
DB_PASSWD = ''
DB_HOST = ''
DB_PORT = '5432'

if args.db_host:
    if args.db_host != 'localhost':
        DB_CONTYPE = "network"
        DB_HOST = args.db_host

if args.db_port:
    DB_PORT = args.db_port

if args.db_user:
    DB_USER = args.db_user

if args.db_passwd:
    DB_PASSWD = args.db_passwd

#==========================
#   FUNCTIONS & CLASSES   #
#==========================


def main():
    # Set up a logging configuration:
    log_fname = '%s/%s-%s' % (LOG_DIR, LOG_PREF, TODAY)
    row_format = '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(format=row_format, filename=log_fname,
                        level=logging.INFO)
    log = logging.getLogger('index_rebuilder')

    #
    # If statistics' arguments have been passed:
    #
    if args.stat or args.invalid or args.scan_counter is not None:
        idx_stat = db.GlobIndexStat(args.dbname)
        idx_stat.set_log(log)
        idx_stat.get_connect(con_type=DB_CONTYPE, host=DB_HOST,
                             pg_port=DB_PORT, user=DB_USER,
                             passwd=DB_PASSWD)

        # Show top of bloated indexes:
        if args.stat:
            idx_stat.print_bloat_top()

        # Show invalid indexes:
        if args.invalid:
            idx_stat.print_invalid()

        # Show unused indexes:
        if args.scan_counter is not None:
            idx_stat.print_unused(args.scan_counter)

        idx_stat.close_connect()
        sys.exit(0)

    #
    # If rebuilding arguments have been passed:
    #

    # For mail reporting:
    mail_report = Mail(ALLOW_MAIL_NOTIFICATION, SMTP_SRV, SMTP_PORT,
                       SMTP_ACC, SMTP_PASS, SENDER, RECIPIENT, SBJ)

    print('Log is collected to %s' % log_fname)

    if args.index:
        index = db.Index(args.index, args.dbname)
        index.set_log(log)
        if index.get_connect(con_type=DB_CONTYPE, host=DB_HOST,
                             pg_port=DB_PORT, user=DB_USER,
                             passwd=DB_PASSWD):
            index.set_lock_query_timeo(LOCK_QUERY_TIMEO)
            stat = index.rebuild()
            index.close_connect()
            if stat:
                report_list.append(stat+'\n')
            else:
                report_list.append('Rebuilding of %s failed. '
                                   'See %s for more info\n' %
                                   (args.index, log_fname))
        else:
            report_list.append('Connection to the database '
                               '%s failed\n' % args.dbname)

    # Rebuild indexes by using index names from a passed file:
    elif args.filename:
        try:
            fp = open(args.filename, 'r')
        except IOError as e:
            print(e)
            sys.exit(e.errno)

        for i in fp:
            if i == '\n':
                continue

            indexname = i.rstrip('\n').strip(' ')

            index = db.Index(indexname, args.dbname)
            index.set_log(log)
            if index.get_connect(con_type=DB_CONTYPE, host=DB_HOST,
                                 pg_port=DB_PORT, user=DB_USER,
                                 passwd=DB_PASSWD):
                index.set_lock_query_timeo(LOCK_QUERY_TIMEO)
                stat = index.rebuild()
                index.close_connect()
                if stat:
                    report_list.append(stat+'\n')
                else:
                    report_list.append('Rebuilding of %s failed. '
                                       'See %s for more info\n' %
                                       (args.index, log_fname))
            else:
                report_list.append('Connection to the database '
                                   '%s failed\n' % args.dbname)
                break

        x = 1
        print("\nSummary:\n========")
        for i in report_list:
            print('[%s] %s' % (x, i), end='')
            x += 1

    mail_message = ''.join(report_list)
    mail_report.send(mail_message)

    sys.exit(0)


if __name__ == '__main__':
    main()
