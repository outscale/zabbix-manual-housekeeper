#!/usr/bin/env python

import mysql.connector
import time
import datetime
import logging
import sys
import settings
from optparse import OptionParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stdout)

parser = OptionParser()
parser.add_option("--node", dest="node", help="SQL node", metavar="")
parser.add_option("--batch", dest="batch", help="How many events to delete by request", metavar="")
parser.add_option("--limit", dest="limit", help="SQL request limit", metavar="")
parser.add_option("--month", dest="month", help="How many month to keep", metavar="")
parser.add_option("--only-trends", dest="trends", help="Delete only trends/trends_uint.", metavar="", action="store_true", default=False)

(options, domain) = parser.parse_args()

if not options.node or not options.batch or not options.limit or not options.month:
	parser.error('Missing parameter(s)')

node = options.node
batch = int(options.batch)
limit = int(options.limit)
month = int(options.month)
trends = options.trends

conn = mysql.connector.connect(host=node,user=settings.DB_USER,password=settings.DB_PASSWORD, database=settings.DATABASE)
cur = conn.cursor()

requests = [ 
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=0 and events.object=0 and not exists (select null from problem where events.eventid=problem.eventid) and not exists (select null from problem where events.eventid=problem.r_eventid) order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=3 and events.object=0 and not exists (select null from problem where events.eventid=problem.eventid) and not exists (select null from problem where events.eventid=problem.r_eventid) order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=3 and events.object=4 and not exists (select null from problem where events.eventid=problem.eventid) and not exists (select null from problem where events.eventid=problem.r_eventid) order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=3 and events.object=5 and not exists (select null from problem where events.eventid=problem.eventid) and not exists (select null from problem where events.eventid=problem.r_eventid) order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=1 and events.object=1 order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=1 and events.object=2 order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where clock<UNIX_TIMESTAMP(now() - interval {} month) and events.source=2 and events.object=3 order by eventid limit {}'.format(month, limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where source = 0 and object = 0 and objectid not in (select triggerid from triggers) limit {};'.format(limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where source = 3 and object = 0 and objectid not in (select triggerid from triggers) limit {};'.format(limit) },
	{ 'table': 'events', 'column': 'eventid', 'request': 'select eventid from events where source = 3 and object = 4 and objectid not in (select itemid from items) limit {};'.format(limit) },
	{ 'table': 'acknowledges', 'column': 'acknowledgeid', 'request': 'select acknowledgeid from acknowledges where not eventid in (select eventid from events) limit {}'.format(limit) },
	{ 'table': 'acknowledges', 'column': 'acknowledgeid', 'request': 'select acknowledgeid from acknowledges where not userid in (select userid from users) limit {};'.format(limit) },
	{ 'table': 'acknowledges', 'column': 'acknowledgeid', 'request': 'select acknowledgeid from acknowledges where eventid in (select eventid from events where (source = 0 or source=3) and object = 0 and objectid not in (select triggerid from triggers)) limit {};'.format(limit) },
	{ 'table': 'acknowledges', 'column': 'acknowledgeid', 'request': 'select acknowledgeid from acknowledges where eventid in (select eventid from events where source=3 and object = 4 and objectid not in (select itemid from items)) limit {};'.format(limit) },
	{ 'table': 'sessions', 'column': 'sessionid', 'request': 'select sessionid from sessions where lastaccess<UNIX_TIMESTAMP(now() - interval 1 month) limit {}'.format(limit) },
	{ 'table': 'auditlog', 'column': 'auditid', 'request': 'select auditid from auditlog where clock<UNIX_TIMESTAMP(now() - interval 1 month) limit {}'.format(limit) }
]

def truncate_housekeeper():
	req = f"truncate table housekeeper;"
	logging.info(req)
	retry = 30
	x = 0
	while x < retry:
		try:
			cur.execute(req)
			conn.commit()
		except Exception as err:
			x+=1
			logging.warning(f"Retry #{x}: {err}")
			time.sleep(5)
			continue
		break
	if x >= retry:
		logging.error("Can't perform requests.")
		exit(1)

def delete_problems():
	req = f"delete from problem where r_clock<>0 and r_clock<UNIX_TIMESTAMP(now() - interval {month} month) limit {limit};"
	logging.info(req)
	retry = 30
	x = 0
	while x < retry:
		try:
			cur.execute(req)
			conn.commit()
		except Exception as err:
			x+=1
			logging.warning(f"Retry #{x}: {err}")
			time.sleep(5)
			continue
		break
	if x >= retry:
		logging.error("Can't perform requests.")
		exit(1)
	logging.info(f"{cur.rowcount} row(s) deleted")


def mass_delete_events(data):
	global batch, limit
	rows_deleted = 0
	if 'limit' in data:
		request = f"{data['request']} limit {data['limit']}"
	else:
		request = data['request']
	logging.info(f"{request}")
	cur.execute(request)
	res = cur.fetchall()
	numrows = cur.rowcount
	ids = []
	if 'batch' in data:
		batch = data['batch']
	if numrows <= batch and numrows > 1:
		for x in range(0, numrows):
			ids.append(res[x][0])
		ids = tuple(ids)
		retry = 30
		y = 0
		while y < retry:
			try:
				del_req = f"delete from {data['table']} where {data['column']} in {ids};"
				logging.debug(del_req)
				cur.execute(del_req)
				conn.commit()
				ids = []
			except Exception as err:
				y+=1
				logging.warning(f"Retry #{y}: {err}")
				continue
			break
		if y >= retry:
			logging.error("Can't perform requests.")
			exit(1)
		rows_deleted += cur.rowcount
	else:
		for x in range(0, numrows):
			ids.append(res[x][0])
			if (x != 0 and x % batch == 0):
				ids = tuple(ids)
				retry = 30
				y = 0
				while y < retry:
					try:
						del_req = f"delete from {data['table']} where {data['column']} in {ids};"
						logging.debug(del_req)
						cur.execute(del_req)
						conn.commit()
						ids = []
					except Exception as err:
						y+=1
						logging.warning(f"Retry #{y}: {err}")
						continue
					break
				if y >= retry:
					logging.error("Can't perform requests.")
					exit(1)
				rows_deleted += cur.rowcount
	logging.info(f"{rows_deleted} row(s) deleted")
				
				

def main():

	if not trends:
		start_time = time.time()
		truncate_housekeeper()
		end_time = time.time()
		time_elapsed = round((end_time - start_time),2)
		logging.info(f"Time elapsed to truncate housekeeper: {time_elapsed}sec")
		start_time = time.time()
		delete_problems()
		end_time = time.time()
		time_elapsed = round((end_time - start_time),2)
		logging.info(f"Time elapsed to delete problems: {time_elapsed}sec")
	for data in requests:
		if trends and data['table'] not in ['trends', 'trends_uint']:
			continue
		start_time = time.time()
		mass_delete_events(data)
		end_time = time.time()
		time_elapsed = round((end_time - start_time),2)
		logging.info(f"Time elapsed to delete {data['column']} in {data['table']}: {time_elapsed}sec")

main()

conn.close()
