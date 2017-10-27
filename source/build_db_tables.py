# -*- coding: utf-8 -*-
#!/usr/bin/python3

import PM

import numpy as np
import pandas as pd

from numpy import nan as NA

import csv,sqlite3
import pandas.io.sql as sql


def create_db():
	conn = sqlite3.connect(PM.db)
	print('creating database ......')
	paper_info = pd.read_csv(PM.paper_info,sep=u'|',header = 0,index_col='paper_index')
	# paper_info = paper_info[paper_info['paper_year'].astype(np.int64)<=2013] # ==> kick.db
	paper_info['paper_year'] = paper_info['paper_year'].astype(np.int64) # ==> comp.db
	paper_info.to_sql('paper_info',conn,index = True)# create table paper_info_e
	conn.commit()
	conn.close()
	print('--'*10 +'db created finished.....'+'--'*10)

# create_db()
# ------------------------------------------------------------------------------------

def create_tab_write():
	# author_name | paper_index | author_place
	conn = sqlite3.connect(PM.db)
	print("creating author_index_place table write.....")
	#	relation table : write {author-paper}
	conn.execute('drop table if exists write')
	create_write_com = 'create table If Not Exists write \
	(row_id INTEGER PRIMARY KEY	AUTOINCREMENT \
	 ,author_name varchar(20) \
	 ,paper_index varchar(20) \
	 ,place INTEGER\
	 )'
	conn.execute(create_write_com)
	as_p = conn.execute('select \
		paper_authors,\
		paper_index \
		from paper_info'# limit 10'
		)	
	pa=[x for x in as_p]
	wrt = [(NA,a,x[1],x[0].split(',').index(a)+1) for x in pa for a in x[0].split(',')]	
	print("building write relation....")
	conn.executemany('insert into write values(?,?,?,?)',wrt)
	print('--'*10 + 'write relation created' +'--'*10)
	conn.commit()
	conn.close()

# create_tab_write()
# ------------------------------------------------------------------------------------

def create_tab_reference():
	#p_citer | p_cited |
	conn = sqlite3.connect(PM.db)
	conn.execute('drop table if exists reference')
	print("create relation table reference .........")
	create_reference_com = 'create table If Not Exists reference \
	(row_id INTEGER PRIMARY KEY	AUTOINCREMENT \
	 ,p_citer varchar(20) \
	 ,p_cited varchar(20) \
	 )'
	conn.execute(create_reference_com)
	ref_p = conn.execute('select \
	 paper_index \
	,paper_reference\
	from paper_info'# limit 100'
	)
	ref =[x for x in ref_p]
	# print(ref)
	refr = []
	for x in ref:
		if type(x[1]) ==str:
			for y in set(x[1].strip().split(',')):
				refr.append((NA,x[0],y))
		else:
			refr.append((NA,x[0],NA))
	print("building table reference relation....")
	conn.executemany('insert into reference values(?,?,?)',refr)
	print('--'*10 + 'reference relation table created' +'--'*10)
	conn.commit()
	conn.close()

# create_tab_reference()
# ------------------------------------------------------------------------------------
def create_tab_author():

	conn = sqlite3.connect(PM.db)
	print('building author table....')
	conn.execute('drop table if exists author')
	ath = conn.execute('select distinct author_name from write')

	author_col = [(NA,x[0]) for x in ath]
	conn.execute('create table If Not Exists author\
		(row_id INTEGER PRIMARY KEY	AUTOINCREMENT \
		,author_name varchar(20))')
	conn.executemany('insert into author values(?,?)',author_col)
	conn.commit()
	print('--'*10 + "building table author name list ...."+'--'*10)

# create_tab_author()
# ------------------------------------------------------------------------------------

def create_author_paper():
	print('create_author_paper .........')
	conn = sqlite3.connect(PM.db)
	qry = 'select author_name,paper_index,place from write'
	author_tab = sql.read_sql_query(qry,conn)
	author_tab.paper_index = author_tab.paper_index.astype(np.int64)
	print("author write" + str(author_tab.shape))

	qry = 'select paper_index,paper_title,paper_year,paper_publish from paper_info'
	pin = sql.read_sql_query(qry,conn)
	print("paper descripe " + str(pin.shape))

	author_des = pd.merge(author_tab,pin,on ='paper_index',how='left')
	author_des['row_id'] = NA
	columns = ['row_id','author_name','paper_index' ,'place' ,'paper_title' ,'paper_year','paper_publish' ]

	author_des= author_des.reindex(columns=columns)
	print(author_des[:1])

	conn.execute('drop table if exists author_paper')
	create_p_a = 'create table If Not Exists author_paper\
		(row_id INTEGER PRIMARY KEY	AUTOINCREMENT \
		,author_name TEXT,paper_index TEXT,place varchar(4)\
		,paper_title TEXT,paper_year varchar(8),paper_publish TEXT) '
	conn.execute(create_p_a)
	print('table created ....')
	ad = [tuple(x) for x in author_des[:-1].values]	

	print('insert to author_paper table.....')
	conn.executemany('insert into author_paper values(?,?,?,?,?,?,?)',ad)

	conn.commit()
	conn.close()



# create_author_paper()

# ------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------


def create_paper_pr_cc_year_pub():
	print('---------- paper_index - paper_rank - citedcount - year - publish -----------')
	conn = sqlite3.connect(PM.db)
	paper_rank = pd.read_csv(PM.paper_rank,sep=u'|',header = 0,index_col='paper_index')
	# 
	qry ='select p_cited as paper_index,p_citer from reference '
	pp_citedcount = sql.read_sql_query(qry,conn)
	pp_citedcount = pp_citedcount.groupby(['paper_index']).size().dropna()
	pp_citedcount = pd.DataFrame(pp_citedcount)
	pp_citedcount.columns = ['paper_citations']
	# # 
	paper_rank.index = paper_rank.index.astype(np.int64)
	pp_citedcount.index = pp_citedcount.index.astype(np.int64)
	paper = pd.merge(paper_rank,pp_citedcount,left_index=True,right_index=True,how='left')
	# #
	qry = 'select paper_index,paper_year,paper_publish from paper_info'
	pp_year = sql.read_sql_query(qry,conn,index_col='paper_index')
	# #
	pp_year['paper_year'] = pp_year['paper_year'].astype(np.int64)
	paper = pd.merge(paper,pp_year,left_index=True,right_index=True,how='left')
	paper.to_sql('paper_pr_cc_year_pub',conn,index = True)# create table paper_info_e
	conn.commit()
	conn.close()
	print(' * -*'*20)

# create_paper_pr_cc_year_pub()

def create_interest_table():
	conn = sqlite3.connect(PM.db)
	print('creating database author interest table ...........')
	interest = pd.read_csv(PM.task2_training_csv,sep=u'|',header = 0)
	interest.to_sql('author_interst',conn,index = True,chunksize=1000)# create table paper_info_e
	conn.commit()
	conn.close()





















