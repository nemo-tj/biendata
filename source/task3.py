#coding=utf-8
from collections import defaultdict
import numpy as np
import pandas as pd
import sqlite3,csv
import pandas.io.sql as sql

# package-depend ： pygraph, python-graph-core
# pip install pygrah
# pip install python-graph-core

from pygraph.classes.digraph import digraph
from pygraph.algorithms.pagerank import pagerank


from numpy import nan as NA
import math

import os
import glob

import PM


# |paper_index | paper_title | paper_authors | paper_year | paper_publish | paper_reference |
def load_paper(src_file,chunck = 20000):
	print("- - - - - - - - - - - begin to load " + src_file)
	paper_column = {'#i':'paper_index',
				'#*':'paper_title',
				'#@':'paper_authors',
				'#t':'paper_year',
				'#c':'paper_conf',
				'#%':'paper_reference'}

	with open(src_file) as f:
		nu = 1 ;
		paper = {}
		columnId =""
		flag = False
		paper_data={}
		for line in f:
			if(line != "\n"):
				f2 = line.strip()[0:2]
				item = line.strip()[2:]
				if (f2== '#i'):
					columnId = line[6:].strip()
				elif f2 == '#%':
					if paper_column['#%'] not in paper.keys():
						paper[paper_column['#%']] = str(item)
					else:
						paper[paper_column['#%']] += ","+str(item)
				else:
					paper[paper_column[f2]] = line[2:].strip()
			else:
				flag = True
			if flag:
				# obj = pd.Series(paper)
				paper_data[columnId] = paper;
				flag = False
				nu +=1
				paper={}
				if nu % chunck == 0:		
					dict2dataframe(paper_data,int(nu/chunck))		
					paper_data={}
		dict2dataframe(paper_data,nu)
		print("- - - - - - - -- - -finished load " + src_file)
		f.close()

def dict2dataframe(paper_data,nu,csv_file=PM.tmp_path):
	columns =['paper_title','paper_authors',
			'paper_year','paper_conf','paper_reference']
	paper_dataframe = pd.DataFrame(paper_data,index=columns)
	paper_dataframe = paper_dataframe.T
	paper_dataframe.index.name = 'paper_index'
	paper_dataframe.reindex(columns = columns)
	csv = csv_file+"tmp-"+str(nu)+".csv"
	paper_dataframe.to_csv(csv,sep='|',header=0)	
	print(csv)


def merge_paper(result, paper= PM.tmp_path):
	print("- - - - -- - - - begin to merge - - - - -  -- - - " + paper)
	csv_list = glob.glob(paper+"/*-*.csv")
	for p in csv_list:
		f = open(p,'r').read()
		with open(result,'a') as t:
			t.write(f)
			print("meger - > " + p)
		os.remove(p)

	print("- - - - - - - - - - - finished merge, the reusult is ---->"+result)
	columns =['paper_index','paper_title','paper_authors',
			'paper_year','paper_publish','paper_reference']
	papers = pd.read_csv(result,sep=u'|',header=None)
	papers.columns  = columns;
	# papers['paper_index'] = papers['paper_index'].map(lambda x: int(x))
	frame = papers.sort_values(by = 'paper_index')
	frame.to_csv(result,sep =u'|',header = 1,index = False)
# -----------------------------------------------------------------------------------------


def transform_src(src_file=PM.task3_papers_txt,paper_info_result=PM.paper_info):
	if glob.glob(paper_info_result):
		os.remove(paper_info_result)
	load_paper(src_file)
	merge_paper(paper_info_result)

# ----------------------------------------------------------------------------------------
# transform_src()
# ----------------------------------------------------------------------------------------

# remove the duplicated part of reference
def remove_dup_ref(paper_info_result=PM.paper_info):
	print('start to read.........')
	pi = pd.read_csv(paper_info_result,sep=u'|',header=0,index_col='paper_index')
	t = pi['paper_reference'].dropna()
	pi['paper_reference'] = t.map(lambda x : ','.join(set(x.split(','))))
	print('write to file.........')
	pi.to_csv(paper_info_result,sep=u'|',index=True,header=1)

# remove_dup_ref()
# ----------------------------------------------------------------------------------------
# ----> paper_info.csv 

# build_db_tables.py and make it easier for data operation


# ----------------------------------------------------------------------------------------
#  author's feature 
# 

class task3_feature():
	"""docstring for task3"""
	def __init__(self):

		self.conn = sqlite3.connect(PM.db)

		print("="*60)
		print('extracting features......')
		print("="*60)
# -------------------------------------------------------------------------
	def pp_place(self,author_tab,i):
		qry = 'select author_name,sum(times) as num_%d\
					from PAPER_PLACE_COUNT\
					where place = %d\
					group by author_name'%(i,i)
		tt = sql.read_sql_query(qry,self.conn,index_col='author_name')
		author_tab['pp_%d_num'%(i)] = pd.Series(tt['num_%d'%(i)])

	def author_paper_place(self,N=10):
		self.conn = sqlite3.connect(PM.db)
		print("------place feature....---")
		query = 'select author_name from author'
		author_tab = sql.read_sql_query(query,self.conn,index_col = 'author_name')	
		# dataframe: author_name as the index
		# self.conn.execute('drop table if exists PAPER_PLACE_COUNT')
		paper_count_qry='create table If Not Exists PAPER_PLACE_COUNT as\
			select author_name,place, count(*) AS times\
			from write\
			group by author_name, place'
		self.conn.execute(paper_count_qry)
		print('create table PAPER_PLACE_COUNT ....')
		# total_pp_num : how many paper he present?
		qry = 'select author_name, sum(times) as total_num \
			from PAPER_PLACE_COUNT\
			group by author_name'
		total_pp = sql.read_sql_query(qry,self.conn,index_col='author_name')

		author_tab['pp_total_num'] = pd.Series(total_pp['total_num'])

		# pp_often_place| pp_often_place_count: usually, his position of the paper?
		qry = 'select author_name,max(times) As often_times,place AS often_place \
			from PAPER_PLACE_COUNT\
			group by author_name'
		often_place = sql.read_sql_query(qry,self.conn,index_col='author_name')
		author_tab['pp_often_place'] = often_place['often_place']
		author_tab['pp_often_place_count'] = often_place['often_times']

		# pp_last_place: usually, the boss's position is the last one!
		qry = 'select author_name, count(*) AS last_place from \
			(select paper_index,author_name,max(place) from write group by paper_index)\
			group by author_name'
		last_place = sql.read_sql_query(qry,self.conn,index_col='author_name')
		author_tab['pp_last_place_count'] = last_place['last_place']

		# 'select * from write a where (select count(*) from write where paper_index = a.paper_index and place > a.place) < 3 order by a.paper_index,a.place desc;'

		qry = 'select author_name,count(*) as last_place_2 from\
			()\
			group by author_name'
		# pp_(i)_num : position= 1,2,...N ?
		for i in range(1,int(N),2):
			self.pp_place(author_tab,i)

		print(author_tab[:2])
		author_tab.to_csv(PM.author_feature_path+'place.csv',sep='|',header=1,index = True)

		self.conn.commit()
		self.conn.close()

	# ------------------------------------------------------------------------------------- 
	def place_rank(self,auth_tabl,i,rk='<='):

		qry = 'select author_name,sum(times) as num_%d\
					from PAPER_PLACE_COUNT\
					where place %s %d\
					group by author_name'%(i,rk,i)
		tt = sql.read_sql_query(qry,self.conn,index_col='author_name')
		f ={}
		f['<=']='top'
		f['>']='under'
		auth_tabl['%s_place_%d_num'%(f[rk],i)] = pd.Series(tt['num_%d'%(i)])
		print('%s_place_%d_num'%(f[rk],i))

	def author_place_rank(self):
		self.conn = sqlite3.connect(PM.db)
		print("------place ranking feature....---")
		query = 'select author_name from author'
		author_tab = sql.read_sql_query(query,self.conn,index_col = 'author_name')	
		for i in range(1,16,1):
			self.place_rank(author_tab,i,rk='<=')
			# print('rank %d ....'%i)
		for i in range(2,21,2):
			self.place_rank(author_tab,i,rk='>')
			# print('rank %d ....'%i)
		print('to csv ........')
		author_tab.to_csv(PM.author_feature_path+'toprank.csv',sep='|',header=1,index = True)

# -----------------------------------------------------------------------------------
	# how many paper the author published in the year of 2013,2012,...:
	# def author_paper_year(self):
	# 	self.conn = sqlite3.connect(PM.db)
	# 	print('------paper----year---csv-------')
	# 	qry = 'select author_name,paper_year\
	# 		from author_paper\
	# 		'# author_pi_place
	# 	year = sql.read_sql_query(qry,self.conn)
	# 	year = year[year['paper_year'].astype(np.int64) >= 2000]
	# 	author_year = year.groupby(['author_name','paper_year']).size().unstack()
	# 	print(author_year[:2])

	# 	author_year.to_csv(PM.author_feature_path+'year.csv',sep=u'|',header =1,index = True)	
# -----------------------------------------------------------------------------------
	def pub_year(self,author_year,year,i,rk = '>=',f = {'>=':'after','<':'before'}):

		year['paper_year'] = year['paper_year'].astype(np.int64)
		if rk == '>=':
			year = year[year['paper_year'] >= i]
		elif rk == '<':
			year = year[year['paper_year'] < i]
		author_year['%s_%d_num'%(f[rk],i)] = year.groupby(['author_name'])['year_count'].agg('sum')
		print('%s_%d_num'%(f[rk],i))

	def author_year_count(self):
		self.conn = sqlite3.connect(PM.db)
		print('publish paper for years ........')
		query = 'select author_name from author'
		author_tab = sql.read_sql_query(query,self.conn,index_col = 'author_name')	
		qry = 'select author_name,paper_year,count(*) as year_count\
			from author_paper group by author_name,paper_year'
		year = sql.read_sql_query(qry,self.conn)
		for i in range(2013,1999,-1):
			self.pub_year(author_tab,year,i,rk='>=')
		for i in range(2015,1980,-3):
			self.pub_year(author_tab,year,i,rk='<')
		print('to csv ..........')
		author_tab.to_csv(PM.author_feature_path+'countyear.csv',sep=u'|',header =1,index = True)	

# -----------------------------------------------------------------------------------------
	# how many publishment the author's papers are ?
	def author_paper_publish(self):
		print('------paper----publish---csv-------')
		self.conn = sqlite3.connect(PM.db)
		qry = 'select author_name,paper_publish,count(*) as pub_count\
			from author_paper \
			group by author_name,paper_publish\
			'# author_pi_place
		publish = sql.read_sql_query(qry,self.conn)
		print(str(publish.shape)+'<------publish ')
		# publish variety, how many kinds publishment the author's paper are?
		tmp = publish.groupby('author_name')['paper_publish'].unique()
		tmp = tmp.groupby('author_name').size()
		print('generate pub information .... ')
		pub = pd.DataFrame(tmp)
		pub.index.name = 'author_name'
		pub.columns = ['author_pub_kinds']
		# publish statistics information
		pub['author_pub_max_times'] = publish.groupby(['author_name'])['pub_count'].agg('max')
		pub['author_pub_mean_times'] = publish.groupby(['author_name'])['pub_count'].agg('mean')
		pub['author_pub_median_times'] = publish.groupby(['author_name'])['pub_count'].agg('median')
		
		pub.to_csv(PM.author_feature_path+'publish.csv',sep=u'|',header =1,index = True)

# ---------------------------------------------------------------------------------------------------
	def author_paper_coauthor(self):
		self.conn = sqlite3.connect(PM.db)
		print("----coauthor--csv----")
		qry = 'select author_name,paper_index\
			from write'
		ai = sql.read_sql_query(qry,self.conn)
		sz = ai.groupby('paper_index').size()
		ai['sz'] = ai['paper_index'].map(sz)
		coauthor_df = ai.groupby('author_name')['sz'].agg([\
			('author_pub_max_coauthor','max'),\
			('author_pub_mean_coauthor','mean'),\
			('author_pub_median_coauthor','median'),\
			('author_pub_min_coauthor','min'),\
			('author_pub_sum_coauthor','sum')])
		coauthor_df.to_csv(PM.author_feature_path+"coauthor.csv",header=1,index=True,sep=u'|')
		print(coauthor_df[:2])		
		
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
	def maiden_work(self):
		print('maiden work...............  ')
		conn = sqlite3.connect(PM.db)
		qry = 'select author_name,paper_index,min(paper_year) as f3_maiden_time\
			from author_paper group by author_name'
		maiden = sql.read_sql_query(qry,conn,index_col = 'author_name')
		print('maiden paper index finished .......!')
		# 
		qry = 'select * from paper_pr_cc_year_pub'
		pr = sql.read_sql_query(qry,conn,index_col='paper_index')
		# 
		print('maiden  + pagerank = ................')
		maiden['paper_index'] = maiden['paper_index'].astype(np.int64)
		pr.index = pr.index.astype(np.int64)
		maiden = pd.merge(maiden,pr,left_on='paper_index',right_index=True,how = 'left')
		print('page rank of maiden work .......')
		# 
		qry = 'select * from publish_rank'
		pub = sql.read_sql_query(qry,conn,index_col='paper_publish')
		maiden = pd.merge(maiden,pub,left_on = 'paper_publish',right_index=True,how='left')
		print('first publishment surely influence the author ....... ')
		# 
		maiden['f3_maiden_time'] = 2017 - maiden['f3_maiden_time'].astype(np.int64)
		maiden = maiden.drop(['paper_index','paper_year','paper_publish'],axis=1)
		maiden.to_csv(PM.author_feature_path+'f3_maiden.csv',sep='|',header=1,index=True)
		print(maiden[:6])
		print(' * -*'*20)

	def guamin_work(self):
		print("gua ming work ............")
		conn = sqlite3.connect(PM.db)
		qry = 'select author_name,count(*) as gm_last_times from (select author_name, paper_index, max(place) from write \
					group by author_name, paper_index) group by author_name'
		gm = sql.read_sql_query(qry,conn,index_col='author_name')
		print(gm[:6])
		gm.to_csv(PM.author_feature_path+'gm_last.csv',header=1,index=True,sep='|')

	def place_k_year_n_citation(self,qry1,qry2,filename,k):
		print('k place  work .......')
		conn = sqlite3.connect(PM.db)
		author = sql.read_sql_query(qry1,conn)
		paper = sql.read_sql_query(qry2,conn)
		author['paper_index'] = author['paper_index'].astype(np.int64)
		paper['paper_index']  = paper['paper_index'].astype(np.int64)
		info = pd.merge(author,paper,left_on = 'paper_index',right_on = 'paper_index',how = 'left')
		group = info.groupby('author_name')
		df1 = group['paper_citations'].agg([
			('max_citation_'+str(k),'max'),
			('sum_citation_'+str(k),'sum'),
			('median_citation_'+str(k),'median')
			])
		df2 = 1e8*group['pp_ranking'].agg([
			('max_pr_'+str(k),'max'),
			('sum_pr_'+str(k),'sum'),
			('median_pr_'+str(k),'median')
			])
		df = pd.concat([df1,df2],axis=1)
		df[str(k)+'_count'] =  group.size()
		df.to_csv(PM.author_feature_path+filename,sep='|',header = 1, index = True)
		print(df[:6])
		print(' * -*'*20)

	def place_1_work_citation(self):
		print('place_1_work_citation ..........')
		qry1 = 'select author_name, paper_index from write where place=1'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_1111_citation.csv',1111)

	def place_1_work_3y_citation(self):
		print('place_1_work_3y_citation ..........')
		qry1 = 'select author_name, paper_index from write where place=1'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014 and paper_year >=2010'
		self.place_k_year_n_citation(qry1,qry2,'f3_13y_citation.csv',13)


	def place_all_work_citation(self):
		print('place_all_work_citation ..............')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_10000_citation.csv',10000)

	def place_all_before_2000_citation(self):
		print('place_all_before_2000_citation ..........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2000'
		self.place_k_year_n_citation(qry1,qry2,'f3_1999_citation.csv',1999)


	def place_all_in_2000_2010citation(self):
		print('place_all_in_2000_2010citation ..........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year >= 2000 and paper_year < 2010'
		self.place_k_year_n_citation(qry1,qry2,'f3_2009_citation.csv',2009)

	def place_all_in_2010_2014citation(self):
		print('place_all_in_2010_2014citation .........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year >= 2010 and paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_2013_citation.csv',2013)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

	def author_cited_count(self):
		self.conn = sqlite3.connect(PM.db)
		print('----author---been---cited-----')
		qry = 'select p_citer , p_cited from reference'
		citation = sql.read_sql_query(qry,self.conn)
		sz = citation.dropna().groupby('p_cited').size()
		cited_times = pd.DataFrame(sz)
		cited_times.columns = ['cited_times']
		cited_times.index.name = 'paper_index'
		# 
		author_tab = sql.read_sql_query('select author_name,paper_index from write',\
										self.conn, index_col='paper_index')
		df = pd.merge(author_tab,cited_times,left_index=True,right_index=True,how='inner')

		print(df.columns)
		author_cited = df.groupby('author_name')['cited_times'].agg([\
			('cited_sum_times','sum'),\
			('cited_mean_times','mean'),\
			('cited_max_times','max'),\
			('cited_median_times','median'),\
			('cited_std_times','std'),\
			('cited_min_times','min')
			])
		author_cited.index.name = 'author_name'
		author_cited.to_csv(PM.author_feature_path+'citedtimes.csv',header=1,index=True,sep=u'|')

	# the quality of the citation ?
	def author_paper_cited(self):
		self.conn = sqlite3.connect(PM.db)
		print("-------paper--citation--info--csv-----")
		qry = 'select p_citer , p_cited from reference'
		er_ed = sql.read_sql_query(qry,self.conn)
		sz = er_ed.dropna().groupby('p_cited').size()
		er_ed['sz'] = er_ed['p_cited'].map(sz)
		citation_df = er_ed.groupby('p_citer')['sz'].agg([\
			('author_pub_max_cited','max'),\
			('author_pub_mean_cited','mean'),\
			('author_pub_median_cited','median'),\
			('author_pub_min_cited','min'),\
			('author_pub_sum_cited','sum')])
	# 	citation_df.to_csv(PM.paper_citation,header=1,index=True,sep=u'|')

	# def author_paper_quality(self):
	# 	citation_df = pd.read_csv(PM.paper_citation,header=0,index_col='p_citer',sep=u'|')
		citation_df.index.name = 'paper_index'
		citation_df.index = citation_df.index.astype(np.int64)
		qry = 'select author_name,paper_index from write'
		author = sql.read_sql_query(qry,self.conn,index_col='paper_index')
		author.index = author.index.astype(np.int64)
		df = pd.merge(author,citation_df,left_index=True,right_index=True,how='left')

		print('start generating information ... ')
		df1 = df.groupby('author_name')['author_pub_max_cited'].agg([\
			('pubmaxcited_max','max'),\
			('pubmaxcited_min','min'),\
			('pubmaxcited_mean','mean'),\
			('pubmaxcited_median','median'),\
			('pubmaxcited_sum','sum'),\
			('pubmaxcited_std','std')
			])
		df2 = df.groupby('author_name')['author_pub_mean_cited'].agg([\
			('pubmeancited_max','max'),\
			('pubmeancited_min','min'),\
			('pubmeancited_mean','mean'),\
			('pubmeancited_sum','sum'),\
			('pubmeancited_std','std')
			])
		df3 = df.groupby('author_name')['author_pub_sum_cited'].agg([\
			('pubsumcited_max','max'),\
			('pubsumcited_min','min'),\
			('pubsumcited_mean','mean'),\
			('pubsumcited_median','median'),\
			('pubsumcited_sum','sum'),\
			('pubsumcited_std','std')
			])

		citation_df = pd.concat([df1,df2,df3],axis=1)

		print('to csv ......')
		citation_df.to_csv(PM.author_feature_path+"refquality.csv",header=1,index=True,sep=u'|')

# -----------------------------------------------------------------------------------------

# tf = task3_feature()

# tf.author_paper_place()
# 
# tf.author_year_count()

# tf.author_paper_publish()

# tf.author_paper_coauthor()

# tf.author_paper_cited()

# tf.author_cited_count()

# -----------------------------------------------------------------------------------------
# paper ranking

def paper_rank():
	print("start page ranking .....")

	dg = digraph()

	conn = sqlite3.connect(PM.db)
	qry = 'select p_citer,p_cited from reference'
	p_id = sql.read_sql_query(qry,conn)
	print(str(p_id.shape)+'<---------p_id')

	citer = p_id.p_citer.unique()
	p_id = p_id.dropna(axis=0)
	cited = p_id.p_cited.unique()
	nd = set(citer).union(set(cited))
	nd = list(nd)

	print('node is created .....')
	# add nodes
	nodes = np.array(nd).astype(np.int64)
	dg.add_nodes(nodes)
	print("add nodes finished .... ")
	# add edges

	edges = [x for x in zip(p_id['p_citer'].astype(np.int64),p_id['p_cited'].astype(np.int64))]
	for ed in edges:
		dg.add_edge(ed)
	print('add edges finished ....')

	pg = pagerank(dg, damping_factor=0.85, max_iterations=100, min_delta=1e-06)
	pprk = pd.DataFrame(pd.Series(pg))
	pprk.columns = ['pp_ranking']
	pprk.index.name = 'paper_index'
	pprk.to_csv(PM.paper_rank,sep=u'|',header=1,index=True)
	print(pprk[:2])

# paper_rank()

def author_rank():
	qry = 'select author_name,paper_index from write'
	conn = sqlite3.connect(PM.db)
	author = sql.read_sql_query(qry,conn,index_col='paper_index')
	author.index = author.index.astype(np.int64)
	pp_rank = pd.read_csv(PM.paper_rank,sep=u'|',header=0,index_col='paper_index')
	pp_rank['pp_ranking'] = pp_rank['pp_ranking']*1e8
	print('reading finished ---> %s and %s'%(author.shape,pp_rank.shape))

	df = pd.merge(author,pp_rank,left_index=True,right_index=True,how='left')
	df = df.groupby('author_name')['pp_ranking'].agg([
			('pr_sum_value','sum'),\
			('pr_mean_value','mean'),\
			('pr_max_value','max'),\
			('pr_median_value','median'),\
			('pr_min_value','min'),\
			('pr_std_value','std')
			])
	df.index.name='author_name'
	df.to_csv(PM.author_feature_path+'authorrank.csv',header=1,index=True,sep=u'|')

# author_rank()

def publish_rank():
	# 
	print('publish_rank ...........')
	conn = sqlite3.connect(PM.db)
	qry = 'select paper_publish, paper_citations \
		from paper_pr_cc_year_pub group by paper_publish'
	pub = sql.read_sql_query(qry,conn)
	pd = pub.groupby('paper_publish')['paper_citations'].agg([
		('citation_sums','sum'),
		('citation_mean','mean'),
		('citation_median','median'),
		('citation_max','max')
		])
	pd['pubpaper_nums'] = pub.groupby('paper_publish').size() 
	nx = pd.index.dropna()
	pd = pd.loc[nx]
	pd.to_sql('publish_rank',conn,index=True)
	# pd.to_csv(PM.publish_rank,header=1,index=True,sep='|')
	print('publish_rank ...........!!')	
	print(pd[:10])

# -----------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------
# --------------------------^--feature of author ^-----------------------------------
# --------------------------|--------------------|-----------------------------------
# --------------------------|--------------------|-----------------------------------
# -----------------------------------------------------------------------------------


def merge_feature(path = [PM.author_feature_path]):
	print('merge feature...')
	conn = sqlite3.connect(PM.db)
	query = 'select author_name from author'
	author_desc = sql.read_sql_query(query,conn,index_col = 'author_name')	
	# 
	fea_list = []
	for x in path:
		fea_list += glob.glob(x+'*.csv')
	# 
	print(fea_list)
	for csv in fea_list:
		df = pd.read_csv(csv,sep=u'|',header=0,index_col='author_name')
		print(csv + '.....')
		author_desc = pd.merge(author_desc,df,left_index=True,right_index=True,how='left')

	# author_desc.to_csv(PM.author_description_path+'authordesc.csv',header=1,index=True,sep=u'|')

	print('feature merge finished, \n start merge label(trainset.csv) ......')
	
	traincsv = pd.read_csv(PM.task3_train_csv,header=0,index_col='AUTHOR')
	print('start to merge columns .....')
	df = pd.merge(author_desc,traincsv,left_index=True,right_index=True,how='right')
	df.index.name='author_name'
	df.to_csv(PM.author_train_tabl,header=1,index=True,sep=u'|')

	print('train set generated ....')

	validationcsv = pd.read_csv(PM.task3_validation_csv,header=0,index_col='AUTHOR')
	df = pd.merge(author_desc,validationcsv,left_index=True,right_index=True,how='right')
	df.index.name ='author_name'
	df.to_csv(PM.author_validation_tabl,header=1,index=True,sep=u'|')
	print('validation set generated...')


# merge_feature()


def sub_task3_file():
	result = pd.read_csv(PM.author_pred_path+'task3.csv',sep='\t',index_col = 'authorname',header =0)
	result.to_csv(PM.sub_task3,sep='\t',header=1,index = True)









class author_feature():
	
	def __init__(self):
		print(' - * -'*20)
		print('time and not time related feature work........ ')
		self
		pass

	def maiden_work(self):
		print('maiden work...............  ')
		conn = sqlite3.connect(PM.db)
		qry = 'select author_name,paper_index,min(paper_year) as f3_maiden_time\
			from author_paper group by author_name'
		maiden = sql.read_sql_query(qry,conn,index_col = 'author_name')
		print('maiden paper index finished .......!')
		# 
		qry = 'select * from paper_pr_cc_year_pub'
		pr = sql.read_sql_query(qry,conn,index_col='paper_index')
		# 
		print('maiden  + pagerank = ................')
		maiden['paper_index'] = maiden['paper_index'].astype(np.int64)
		pr.index = pr.index.astype(np.int64)
		maiden = pd.merge(maiden,pr,left_on='paper_index',right_index=True,how = 'left')
		print('page rank of maiden work .......')
		# 
		qry = 'select * from publish_rank'
		pub = sql.read_sql_query(qry,conn,index_col='paper_publish')
		maiden = pd.merge(maiden,pub,left_on = 'paper_publish',right_index=True,how='left')
		print('first publishment surely influence the author ....... ')
		# 
		maiden['f3_maiden_time'] = 2017 - maiden['f3_maiden_time'].astype(np.int64)
		maiden = maiden.drop(['paper_index','paper_year','paper_publish'],axis=1)
		maiden.to_csv(PM.task3_year_related_path+'f3_maiden.csv',sep='|',header=1,index=True)
		print(maiden[:5])
		print(' * -*'*20)

	def place_k_year_n_citation(self,qry1,qry2,filename,k):
		print('k place  work .......')
		conn = sqlite3.connect(PM.db)
		author = sql.read_sql_query(qry1,conn)
		paper = sql.read_sql_query(qry2,conn)
		author['paper_index'] = author['paper_index'].astype(np.int64)
		paper['paper_index']  = paper['paper_index'].astype(np.int64)
		info = pd.merge(author,paper,left_on = 'paper_index',right_on = 'paper_index',how = 'left')
		group = info.groupby('author_name')
		df1 = group['paper_citations'].agg([
			('max_citation_'+str(k),'max'),
			('sum_citation_'+str(k),'sum'),
			('median_citation_'+str(k),'median')
			])
		df2 = 1e8*group['pp_ranking'].agg([
			('max_pr_'+str(k),'max'),
			('sum_pr_'+str(k),'sum'),
			('median_pr_'+str(k),'median')
			])
		df = pd.concat([df1,df2],axis=1)
		df[str(k)+'_count'] =  group.size()
		df.to_csv(PM.task3_year_related_path+filename,sep='|',header = 1, index = True)
		print(df[:10])
		print(' * -*'*20)

	def place_1_work_citation(self):
		print('place_1_work_citation ..........')
		qry1 = 'select author_name, paper_index from write where place=1'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_1111_citation.csv',1111)

	def place_1_work_3y_citation(self):
		print('place_1_work_3y_citation ..........')
		qry1 = 'select author_name, paper_index from write where place=1'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014 and paper_year >=2010'
		self.place_k_year_n_citation(qry1,qry2,'f3_13y_citation.csv',13)


	def place_all_work_citation(self):
		print('place_all_work_citation ..............')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_10000_citation.csv',10000)

	def place_all_before_2000_citation(self):
		print('place_all_before_2000_citation ..........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year < 2000'
		self.place_k_year_n_citation(qry1,qry2,'f3_1999_citation.csv',1999)


	def place_all_in_2000_2010citation(self):
		print('place_all_in_2000_2010citation ..........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year >= 2000 and paper_year < 2010'
		self.place_k_year_n_citation(qry1,qry2,'f3_2009_citation.csv',2009)

	def place_all_in_2010_2014citation(self):
		print('place_all_in_2010_2014citation .........')
		qry1 = 'select author_name, paper_index from write'
		qry2 = 'select * from paper_pr_cc_year_pub where paper_year >= 2010 and paper_year < 2014'
		self.place_k_year_n_citation(qry1,qry2,'f3_2013_citation.csv',2013)

