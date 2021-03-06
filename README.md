index_rebuilder.py - the utility rebuilds
PostgreSQL indexes without table locking and shows related index statistics

Author: Andrey Klychkov aaklychkov@mail.ru

Version: 2.4.3

Date: 20-08-2018

### Requirements:
```Python3+, psycopg2, pyyaml```

Also needs to create a configuration file (see the index_rebuilder.conf.example)

SQL templates are stored in index_rebuilder_sql.yml

### Descriprion:
index_re.py - rebuilds postgresql indexes (concurrently) or shows:
```
1) top of bloated indexes (may be you want to know which indexes should be rebuilt first)
2) invalid indexes (it's a good idea to check them after mass rebuilding)
3) unused indexes (perhaps unused bloated indexes will be found, so better if you remove them at all)
```
### Understanding concurrent index rebuilding:

To rebuild a postgresql index in concurrent mode
(without a table locking), it needs to do the steps below:
```
1) check validity of the current index
2) get the index definition from pg_indexes view;
3) get the index comment if exists;
4) realize a temporary name for a new index;
5) make the creation command by using the index definition
   and the temporary index name, add expression 'CONCURRENTLY'
   after 'CREATE INDEX'
6) create the new index by using the creation command
7) add a comment if the old index has it
8) check new index validity
9) if the new index is valid, drop the old index in concurrent mode
10) rename the new index like the old index
```
### Configuration:

Configuration file allows to set up:
- statement timeout for dropping\altering commands, otherwise the table can be locked indefinitely
- path to the log file
- mail notifications about job results

**Important:** During execution ALTER INDEX commands a table is locked and all queries won't be executed until that the commands are in progress. To avoid queries queues, the statement_timeout value must be set up into the utility configuration file (initially set to 10 seconds). After specified time is over, the command will be interrupted (that you'll see in the log) and it needs to be done manually by using psql/PgAdmin, for example. See "Understanding concurrent index rebuilding" above. You may change the statement_timeout value by adding a desired value to the configuration file.

### Mail notification:

If mail notifications are allowed, you'll see a job report that contents the line like below for each rebuilded index:
```
index_name: done. Size (in bytes): prev 16384, fin 16383, diff 1, exec time 0:00:00.069175
```
### Logging:

Example event log file entries:
```
2018-04-10 14:42:21,855 [INFO] Connection to database otp_db established
2018-04-10 14:42:21,856 [INFO] Start to rebuild of test0_name_idx, current size: 16384 bytes
2018-04-10 14:42:21,857 [INFO] Index is valid
2018-04-10 14:42:21,860 [INFO] Try: CREATE INDEX CONCURRENTLY new_test0_name_idx ON public.test0 USING btree (name)
2018-04-10 14:42:21,862 [INFO] Creation has been completed
2018-04-10 14:42:21,862 [INFO] New index new_test0_name_idx is valid, continue
2018-04-10 14:42:21,862 [INFO] Try to drop index test0_name_idx
2018-04-10 14:42:21,864 [INFO] Dropping done
2018-04-10 14:42:21,864 [INFO] Set statement timeout '30s': success
2018-04-10 14:42:21,864 [INFO] Try to rename index new_test0_name_idx to test0_name_idx
2018-04-10 14:42:21,864 [INFO] Renaming is done
2018-04-10 14:42:21,864 [INFO] Reset statement timeout to '0': success
2018-04-10 14:42:21,865 [INFO] test0_name_idx: done. Size (in bytes): prev 16384, fin 16384, diff 0, exec time 0:00:00.009829
```

If the --verbose arg has been passed, you'll also see log messages on the console, for example:
```
2018-04-12 10:05:40.385696 : Connection to database otp_db established
2018-04-12 10:05:40.387331 : Start to rebuild of test0_name_idx, current size: 16384 bytes
2018-04-12 10:05:40.387985 : Index is valid
2018-04-12 10:05:40.392124 : Try: CREATE INDEX CONCURRENTLY new_test0_name_idx ON public.test0 USING btree (name)
2018-04-12 10:05:40.395398 : Creation has been completed
2018-04-12 10:05:40.396066 : New index new_test0_name_idx is valid, continue
2018-04-12 10:05:40.396235 : Try to drop index test0_name_idx
2018-04-12 10:05:40.399109 : Dropping done
2018-04-12 10:05:40.399184 : Set statement timeout '5s': success
2018-04-12 10:05:40.399246 : Try to rename index new_test0_name_idx to test0_name_idx
2018-04-12 10:05:40.399944 : Renaming is done
2018-04-12 10:05:40.400282 : Reset statement timeout to '0': success
2018-04-12 10:05:40.401007 : test0_name_idx: done. Size (in bytes): prev 16384, fin 16384, diff 0, exec time 0:00:00.015024
```

### Synopsis:
```
index_rebuilder.py [-h] -c FILE -d DBNAME [-p PORT] [-H HOST] [-U USER] [-P PASSWD]
                   [--verbose] [-s | -u SCAN_COUNTER | -i | -n | -r INDEX | -f FILE | --version]
```

**Options:**
```
  -h, --help            show this help message and exit
  -c FILE, --config FILE
                        path to configuration FILE
  -d DBNAME, --datbase DBNAME
                        database name
  -p PORT, --port PORT  database port
  -H HOST, --host HOST  database host
  -U USER, --user USER  database user
  -P PASSWD, --passwd PASSWD
                        db user password
  -s, --stat            show top of bloated indexes
  -u SCAN_COUNTER, --unused SCAN_COUNTER
                        show unused indexes with SCAN_COUNTER
  -i, --invalid         show invalid indexes
  -n, --new             show indexes with 'new_' prefix
  -r INDEX, --rebuild INDEX
                        rebuild a specified index
  -f FILE, --file FILE  rebuild indexes from FILE
  --verbose             print log messages to the console
  --version             show version and exit
```


**Examples:**

Show the top of bloated indexes:
```
./index_rebuilder.py -d mydbname -s -c /path/to/file.conf
```
Show unused indexes that have usage counter equal or less than 10:
```
./index_rebuilder.py -d mydbname -u 10 -c /path/to/file.conf
```

Show invalid indexes:
```
./index_rebuilder.py -d mydbname -i -c /path/to/file.conf
```

Rebuild some index:
```
./index_rebuilder.py -d mydbname -r my_bloated_index -c /path/to/file.conf
```

Rebuild indexes from a file:
```
./index_rebuilder.py -d mydbname -f file_with_indexnames -c /path/to/file.conf
```
