#!/usr/bin/env python3
from collections import defaultdict
import numpy as np
import pandas as pd

from numpy import nan as NA
import sqlite3,csv
import pandas.io.sql as sql

import math

import matplotlib.pyplot as plt


import os
import glob

import PM


def build_labelcsv():
	interest = []
	with open(PM.task2_labels_file) as labl:
		for w in labl:
			interest.append(w.strip())
	labl.close()
	lablcsv = pd.DataFrame(columns= interest)
	lablcsv.index.name= 'author_name'	
	lablcsv.to_csv(PM.task2_labels_csv,sep=u'|',header = 1,index = True)

# # build_labelcsv()

def build_traincsv():
	tab = {'author_name':[],'interest':[]}		
	with open(PM.task2_training_file) as tr:
		i = 0
		for x in tr:
			if(x.strip()):
				if(i == 0):
					author = x.strip()
					i = 1
				else:
					ist = {}
					for y in x.strip().split(','):
						tab['author_name'].append(author)
						tab['interest'].append(y)
					i  = 0
	traincsv = pd.DataFrame(tab)
	traincsv.to_csv(PM.task2_training_csv,sep=u'|',header = 1,index = False)
	print(traincsv.columns)


def build_validcsv():
	valname = []
	with open(PM.task2_validation_file) as val:
		for x in val:
			t = x.strip()
			if(t):
				valname.append(t)
	df = pd.DataFrame(pd.Series(valname),columns=['author_name'])
	df.to_csv(PM.task2_validation_csv,header=1,index=False)
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

def create_publish_interest():
	print('mark the interest of publishment .......')
	conn = sqlite3.connect(PM.db)
	qry = 'select author_name,paper_publish,count(*) as weight from author_paper\
		group by author_name,paper_publish'	
	author_pub = sql.read_sql_query(qry,conn)	
	qry = 'select author_name,interest as topic from author_interst'
	author_inst =sql.read_sql_query(qry,conn)
	print('merge internal table ........')
	pub_inst = pd.merge(author_inst,author_pub,on='author_name',how = 'left')
	# ------> author_name,paper_publish,interest,weight
	# calculate the interest of publish !
	dic = pub_inst.groupby(['paper_publish','topic']).sum().reset_index()
	dic.to_sql('publish_topic',conn)

	print(dic[:10])
	dic.to_csv(PM.publish_topic,header=1,index=False,sep='|')
	conn.commit()
	conn.close()

def publish_2_topic():
	print('publish - > topic ..... ')
	conn = sqlite3.connect(PM.db)
	qry ='select paper_publish,topic,weight from publish_topic '
	topic = sql.read_sql_query(qry,conn)
	# print(topic[:10])
	print(' * *'*20)
	val = pd.DataFrame(index = pd.Series(topic['paper_publish'].unique()))
	val['groupsumw'] = topic.groupby('paper_publish')['weight'].agg('sum')
	topic = pd.merge(topic,val,left_on='paper_publish',right_index = True,how='left')
	topic['topicweight'] = topic['weight']/(topic['groupsumw']*1.0)
	print(topic[:9])
	conn.execute('drop table if exists publish_topic_weight')
	topic.to_sql('publish_topic_weight',conn)
	print(topic.columns)
	print(topic.shape)
	conn.commit()
	conn.close()

def author_2_publish_2_topic():
	print('* * '*20)
	conn = sqlite3.connect(PM.db)
	qry = 'select author_name,paper_publish,count(*) as counts \
		from author_paper group by author_name,paper_publish '
	author = sql.read_sql_query(qry,conn,index_col=['author_name'])
	# author_name | paper_publish | counts |
	# 
	print(' * *'*20)
	validation = pd.read_csv(PM.task2_validation_csv,header=0,index_col='author_name')
	author = author.loc[validation.index]
	author['author_name'] = author.index
	# validation authors ......
	# 
	print('=======> author table --> ' + str(author.shape))
	qry = 'select paper_publish, topic,topicweight \
		from publish_topic_weight'
	publish = sql.read_sql_query(qry,conn)
	# paper_publish | topic | topicweight
	print('merge for author_name, paper_publish,counts, topic, topicweight')
	pa = pd.merge(author,publish,on = 'paper_publish',how='left')
	pa = pa.dropna()
	# author_name | paper_publish | counts |--| paper_publish | topic | topicweight
	# author_name (counts) --> paper_publish --> topic (value)
	# 
	pa['author_tw'] = pa['counts']*pa['topicweight']
	topic = pa.groupby(['author_name','topic'])['author_tw'].sum().reset_index()
	# author_name | topic | author_tw
	print(topic.columns)
	print(' * *'*20)
	# 
	def topk(df,k = 5,columns = 'author_tw'):
		return df.sort_values(by='author_tw')[-k:]
	# 
	f = topic.groupby(['author_name']).apply(topk)
	columns = ['author_name','topic']
	r = f.loc[:,columns]
	r.columns = ['author_name','interest']
	txt = pd.DataFrame(index=pd.Series(r.author_name.unique()))
	txt.index.name = 'authorname'
	txt['ist'] = r.groupby('author_name')['interest'].agg(lambda x: ','.join(x))
	txt['ist'] = txt['ist'].map(lambda x: x+',,,,')
	# 
	columns = ['interest%i'%i for i in range(1,6)]
	for i in range(1,6):
		txt[columns[i-1]] = txt['ist'].map(lambda x: x.split(',')[i-1])
	txt = txt.drop('ist',axis=1)
	print(' * *'*20)
	print(txt[:10])
	txt.to_csv(PM.task2_target,sep='\t',header=1,index=True)
	conn.close()


def sub_task2_file():
	validation = pd.read_csv(PM.task2_validation_csv,header=0,index_col='author_name')
	txt = pd.read_csv(PM.task2_target,sep='\t',header=0,index_col='authorname')
	csv = txt.loc[validation.index].fillna(method='bfill')
	csv.index.name = 'authorname'
	csv.to_csv(PM.sub_task2,sep='\t',header=1,index=True)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 




