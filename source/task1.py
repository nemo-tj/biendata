# -*- coding: utf-8 -*-
# !/usr/bin/python3

import pandas as pd
import numpy as np

from numpy import nan as NA
import re
import os

import requests
from bs4 import BeautifulSoup
import urllib3
from collections import defaultdict

import PM
import task1craw

def build_validCSV():
	memento = {'#id':[],'#name':[],'#org':[],'#search_results_page':[]
		# '#homepage':[],'#pic':[],'#email':[],'#gender':[],'#position':[],'#location':[]
	}
	with open(PM.task1_validation_txt) as file:
		for line in file:
			if line:
				item = line.split(":",1)
				key = item[0].strip()
				if(len(item) == 1):
					value = NA
				else:
					value = item[1].strip()
					memento[key].append(value)
	file.close()
	schoolar_pic  = pd.DataFrame(dict(memento),columns =['#id','#name','#org','#search_results_page'])
	print("finish building memento!")
	schoolar_pic.to_csv(PM.task1_validation_csv,sep=u'|',header = 1, index = False)

def build_trainCSV():

	memento = {'#id':[],'#name':[],'#org':[],'#search_results_page':[],
		'#homepage':[],'#pic':[],'#email':[],'#gender':[],'#position':[],'#location':[]
	}
	with open(PM.task1_training_txt) as file:
		for line in file:
			if line:
				item = line.split(":",1)
				key = item[0].strip()
				if(len(item) == 1):
					value = NA
				else:
					value = item[1].strip()
					memento[key].append(value)
	file.close()
	columns = ['#id','#name','#org','#search_results_page','#homepage','#pic','#email','#gender','#position','#location']
	schoolar_pic  = pd.DataFrame(dict(memento),columns =columns)
	print("finish building memento!")
	schoolar_pic.to_csv(PM.task1_training_csv,sep=u'|',header = 1, index = False)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

#  raw crawl:

def crawl_valid_srp_url():
	cr = task1craw.Crawl()
	valid = pd.read_csv(PM.task1_validation_csv,sep='|',header=0,index_col='#id')
	exp_id = valid.index
	srp = valid['#search_results_page']
	cr.valid_search_result_page(exp_id,srp)


def crawl_train_srp_url():
	cr = task1craw.Crawl()
	train = pd.read_csv(PM.task1_training_csv,sep='|',header=0,index_col='#id')
	exp_id = train.index
	srp = train['#search_results_page']
	cr.train_search_result_page(exp_id,srp)
	if len(cr.brokenurl) == 0:
		return
	for i in range(3):
		brk = pd.DataFrame(pd.Series(cr.brokenurl))
		cr.brokenurl = {}
		brk.columns =['url']
		cr.valid_home_page(brk.index,brk.url)
		print(' * *'*20)

# # # # # # # # # # # # # # # # # # # # 
# # # # 
def raw_build_homepage_url():
	print(' * *'*20)
	print('raw_build_homepage_url() begin ...... ')
	print('raw parse for homepage_url in the google list page ....')
	print(' * *'*20)
	par = task1craw.Paser(PM.task1_validgoogle_path)
	valid = pd.read_csv(PM.task1_validation_csv,sep='|',header=0,index_col='#id')
	# from search result page ! 
	homeurl = par.raw_home_parse(valid.index,valid['#name'],valid['#org'])
	# - - > list of homepage_url
	print(' * *'*20)
	print('to ans path folder - > %s ......' %PM.task1_homeurl_csv)
	homeurl.to_csv(PM.task1_homeurl_csv,sep='|',index=True,header=1)
	print(' * *'*20)


def crawl_valid_home_page():
	cr = task1craw.Crawl()
	valid = pd.read_csv(PM.task1_homeurl_csv,sep='|',header=0,index_col='expert_id')
	print(' * *'*20)
	print('homepage number : -> '+str(valid.shape))
	print(' * *'*20)
	valid = valid.dropna()
	exp_id = valid.index
	homeurl  = valid['homepage_url'].map(lambda url: '/'.join(url.split('/')[:-1]) if (url[-3:]).lower() == 'pdf' else url)
	cr.valid_home_page(exp_id,homeurl)
	# - - > PM.task1_valid_home_path
	# broken url which not succeed! just try again ang again !
	print('broken ones try it again' +'* - *'*15)
	print('= + ='*20)
	if len(cr.brokenurl) == 0:
		return
	for i in range(3):
		brk = pd.DataFrame(pd.Series(cr.brokenurl))
		cr.brokenurl = {}
		brk.columns =['url']
		cr.valid_home_page(brk.index,brk.url)
		print(' * *'*20)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# parser

def parse_homepage_for_email():
	print('parse_homepage_for_email() .....')
	home = pd.read_csv(PM.task1_validation_csv,index_col='#id',header=0,sep='|')
	ids = home.index
	nas= home['#name']
	par = task1craw.Paser(PM.task1_valid_home_path)
	print(' * *'*20)
	print('start parsing email .......')
	email = par.email_from_homeurl_file(ids,nas)
	print('finish parsing ........')
	email = email.dropna()
	email.to_csv(PM.task1_email_csv,index=True,header=1,sep='|')
	print(' * *'*20)

def parse_homepage_for_personphoto():
	print('parse_homepage_for_personphoto() .......')
	par = task1craw.Paser(PM.task1_valid_home_path)
	print(' * *'*20)
	home = pd.read_csv(PM.task1_homeurl_csv,index_col='expert_id',header=0,sep='|')
	ids = home.index
	urls = home['homepage_url'].fillna(0)
	print('start parsing person_photo .......')
	person_photo = par.pic_from_homeurl_file(ids,urls)
	person_photo = person_photo.dropna()
	person_photo.to_csv(PM.task1_personpic_csv,index=True,header=1,sep='|')
	print(' * *'*20)

def parse_homepage_for_position():
	print('parse_homepage_for_position() ........')
	pos = {'associated professor','assistant professor','chair professor',
		'Distinguished professor','Endowed professor','senior lecturer',
		'assistant lecturer','Reader',
		'senior research fellow','assistant research fellow',
		'teaching assistant','research scientist',
		'chancellor','president','chair','director','manager',
		'principal','registrar','head','dean','secretary','editor','officer'}
	par = task1craw.Paser(PM.task1_valid_home_path)
	position = par.position_from_homeurl_file(pos)
	position.to_csv(PM.task1_position_csv,index=True,header=1,sep='|')
	print(' * *'*20)

def parse_homeurl_for_location():
	print('parse_homeurl_for_location() ..........')
	home = pd.read_csv(PM.task1_homeurl_csv,index_col='expert_id',header=0,sep='|')
	loc = home['homepage_url'].map(lambda x: x.strip('/').split('/')[2] if type(x) == str else 'com')
	# print(loc)
	dic = {
		'cn':'China PR',
		'hk':'Hong Kong',
		'jp':'Japan',
		'us':'USA',
		'il':'Israel',
		'ca':'Canada',
		'tw':'Tai Wan',
		'it':'Italy',
		'nl':'The Netherlands',
		'uk':'UK',
		'de':'Germany',
		'com':'USA',
		'edu':'USA',
		'cz':'Czech Republic',
		'au':'Australia',
		'be':'Belgium',
		'fr':'France',
		'gr':'Greece',
		'eu':'EU',
		'se':'Sweden',
		'ae':'United Arab Emirates'
	}
	def fmap(x):
		if x in dic:
			return dic[x]
		else:
			return ''
	home['location'] = loc.map(lambda x: fmap(x.split('.')[-1]))
	location = home.drop('homepage_url',axis=1)
	location.to_csv(PM.task1_location_csv,index=True,header=1,sep='|')
	print(' * *'*20)


def decide_name_sex(name):
	# ending characters:
	# return 'm'
	p1  = set('b,c,d,k,p,q,t,r,o'.split(','))
	p2 = set('an,am,or,ed,ck,in,on,ah,as,os,us,en,el,ew,er,as'.split(','))
	p3 = set('ian,ing,eng,ang'.split())
	if name[-3:] in p3:
		return 'm'
	elif name[-2:] in p2:
		return 'm'
	elif name[-1] in p1:
		return 'm'
	# woman
	w2 = set('na,ie,ly,ey,ia,ey,is'.split(','))
	w3 = set('ine,lla'.split(','))
	if name[-3:] in w3:
		return 'f'
	elif name[-2:] in w2:
		return 'f'
	# character length:
	if len(name) < 5:
		return 'm'
	# 
	return 'm'

def parse_name_for_gender():
	print(' * - *'*20)
	print('gender_predict() .........')
	d = pd.read_csv(PM.task1_validation_csv,header=0,index_col='#id',sep='|')
	d['#sex'] = d['#name'].map(lambda x: decide_name_sex(x.split()[1] if '.' in x.split()[0] else x.split()[0]))
	# f = d['#sex'] + d['#gender']
	# f = f.dropna()
	# print(f.shape)
	# y = f.map(lambda x : 0 if x[0] == x[1] else 1)
	# print(y[ y > 0].shape)

	df = pd.DataFrame(d['#sex'])
	df.columns = ['gender']
	df.index.name = 'expert_id'
	df.to_csv(PM.task1_gender_csv,sep='|',header = 1, index = True)




def task1_result():
	print(' * *'*20)
	print('task1_result() - > submission work task1.csv')
	print(' * *'*20)
	valid = pd.read_csv(PM.task1_validation_csv,sep='|',header=0,index_col='#id')
	# home_url
	# filter the home_url, the likely home url
	result = pd.read_csv(PM.task1_homeurl_csv,sep='|',header=0,index_col='expert_id')
	result = result.loc[valid.index]
	# 
	print('result [homepage_url,location,person_photo,email,gender]-> %s'%PM.sub_task1)
	result.index.name = 'expert_id'
	# gender
	result['gender'] = 'm'
	# position
	position = pd.read_csv(PM.task1_position_csv,sep='|',header=0,index_col='expert_id')
	result['position'] = position['position']
	# person_photo
	person_photo = pd.read_csv(PM.task1_personpic_csv,sep='|',header=0,index_col='expert_id')
	result['person_photo'] = person_photo['person_photo']
	# email
	email = pd.read_csv(PM.task1_email_csv,sep='|',header=0,index_col='expert_id')
	result['email'] = email['email']
	# location
	location = pd.read_csv(PM.task1_location_csv,sep='|',header=0,index_col='expert_id')
	result['location'] =location['location']
	# # #	# # #	# # #	# # #	# # #	# # #	# # #	# # #	# # #	# # #	# # #
	columns = ['homepage_url','gender','position','person_photo','email','location']
	result = result.loc[:,columns]
	result.to_csv(PM.sub_task1,header=1,index=True,sep = '\t')
	print(' * *'*20)

