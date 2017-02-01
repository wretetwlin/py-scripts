#!/usr/local/bin/python3
'''The script require beautifulsoup 4'''
import os,requests,re,datetime,pysolr,MySQLdb.cursors
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

def process(core,entity):

	file_path = os.path.expanduser('~')+'/sh/log.txt'
	kwargs = {'port':8983,'core':core}
	'''create file if not exist.'''
	if not os.path.exists(file_path):
		file = open(file_path,'w+')

	'''Handle delta import'''
	try:
		result = requests.get('''http://localhost:{port}/solr/{core}/dataimport?command=status'''.format(**kwargs))
		soup = BeautifulSoup(result.content, 'lxml')
		for item in soup.find_all('str', attrs={'name': 'status'}):
			if not re.compile(r'idle').match(item.text) == None:
				result = requests.get('''http://localhost:{port}/solr/{core}/dataimport?command=delta-import&clean=false'''.format(**kwargs))
				soup = BeautifulSoup(result.content,'lxml')
				with open(file_path,'a+') as f:
					f.write('''\n-----------log on:[{}]-------------\n'''.format(datetime.datetime.utcnow()))
					f.write(soup.prettify())
					f.write('\n---------------[end log block]---------------\n')
	except Exception:
		with open(file_path,'a+') as f:
			f.write('''port:{port}, solr internal server error 500.\n'''.format(**kwargs))

	'''Handle auto update'''
	db = MySQLdb.connect(host='localhost',db='solr',user='root',passwd='',cursorclass=MySQLdb.cursors.DictCursor)
	cursor = db.cursor()
	cursor.execute('''select * from cores_update_table where core_name="{}" '''.format(kwargs['core']))
	result = cursor.fetchall()
	if len(result) > 0: last_update_time = result[0]['last_update_time']

	cursor.execute('''select * from {} where last_modified > "{}" '''.format(entity,last_update_time))
	results = cursor.fetchall()
	solr = pysolr.Solr('''http://localhost:{port}/solr/{core}'''.format(**kwargs))


	'''Add or update multiple records.'''
	commit = solr.add(results)
	if len(results) > 0:
		cursor.execute('''update cores_update_table set last_update_time = now() where core_name="{}" '''.format(kwargs['core']))
		db.commit()
	db.close()

	return '''finished {} sync.\n'''.format(core)

pool = ThreadPoolExecutor(5)
futures = []
cores = ['gettingstarted','test']
entities = {'gettingstarted':'user', 'test':'test'}
for core in cores:
	futures.append(pool.submit(process, core,entities[core]))

for x in as_completed(futures):
	file_path = os.path.expanduser('~') + '/sh/log.txt'
	with open(file_path,'a+') as f:
		f.write(x.result())