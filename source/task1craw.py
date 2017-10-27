# -*- coding: utf-8 -*-
# !/usr/bin/python3

import pandas as pd
import numpy as np

from numpy import nan as NA
import re

import os
import glob
import shutil

import requests
from bs4 import BeautifulSoup
import urllib
from urllib.parse import urljoin 
import urllib3
import pycurl

import PM
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from threading import Thread
from queue import Queue
from time import sleep

import Levenshtein 

class Crawl():
	'''
	 crawl papge
	 '''
	def __init__(self):

		self.validpagelist = PM.task1_validgoogle_path
		self.trainpagelist = PM.task1_traingoogle_path
		self.validhomepage = PM.task1_valid_home_path
		self.brokenurl = {}

	def crawler(self,uid,url,path):
		user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36"
		headers={"User-Agent":user_agent}
		req = urllib.request.Request(url.strip(), headers=headers)  
		Max_Num = 6  
		html = None
		for i in range(Max_Num):  
			try:  
				html =  urllib.request.urlopen(req,timeout=5)
				html = html.read()
				with open(path+str(uid),'w') as hp:
					hp.write(str(html))
					print('- * - '*20)
				break  
			except Exception as e:
				if i < Max_Num-1:
					sleep(1) 
					continue  
				else :
					self.brokenurl[uid] = url  
					print(e)
		# # ## # ## # ## # ## # ## # ## # ## # ## # ## # ## # ## # #
		# try:
		# 	html = urllib.request.urlopen(url.strip(),timeout=5)
		# except Exception as e:
		# 	self.brokenurl[uid] = url.strip()
		# 	# sleep(1)
		# 	print(e)
		# try:
		# 	html = html.read()
		# 	# sleep(1)
		# 	with open(path+str(uid),'w') as hp:
		# 		hp.write(str(html))
		# 		print(' Y '*30)
		# except Exception as e:
		# 	print('writing error .........')

	def multiThread(self,ids,urls,path,func):
		print('- * '*20)
		q = Queue()
		NUM = 8
		JOBS = zip(ids,urls)
		files =  os.listdir(path)
		# 
		def do_somthing_using(arguments):
			x= arguments
			if x[0] not in files:
				print('=====>' + str(x)+ '.....')
				func(x[0],x[1],path)

		def working():
		    while True:
		        arguments = q.get()
		        do_somthing_using(arguments)
		        # sleep(1)
		        q.task_done()

		#fork NUM个线程等待队列
		for i in range(NUM):
		    t = Thread(target=working)
		    t.setDaemon(True)
		    t.start()

		#把JOBS排入队列
		for i in JOBS:
		    q.put(i)
		#等待所有JOBS完成
		q.join()
		print('- * '*20)

	def valid_search_result_page(self,ids,urls):
		# items = zip(ids,urls)
		# for x, y in items:
		# 	self.crawler(x,y)
		# 	print('=====>' + str(y)+ '.....')
		print('crawl valid csv google search result page url - - >')
		self.multiThread(ids,urls,path=self.validpagelist,func=self.crawler)

	def train_search_result_page(self,ids,urls):
		print('crawl train csv google search result page url - - >')
		self.multiThread(ids,urls,path=self.trainpagelist,func=self.crawler)
	
	def valid_home_page(self,ids,urls):
		print('craw homepages of validations ......')
		self.multiThread(ids,urls,path=self.validhomepage,func=self.crawler)

class Paser():
	"""docstring for Paser"""
	def __init__(self,htmlpath):
		self.htmlpath = htmlpath
		self.expert_id = []
		self.page_info = ''
		self.homepage_url = {}
		self.gender = {}
		self.position={}
		self.person_photo={}
		self.email = {}
		self.location = {}
	# 1. decide which is homepage_url on the search result pages
	# 2. email with reg expression
	# 3. gender, position ...

	def raw_home_parse(self,ids,names,orgs):
		xyz = zip(ids,names,orgs)
		dic = {}
		for m in xyz:
			key = m[0]
			value=[m[1],m[2]] # name,org
			dic[key]=value
		# homepage_dic - -> DataFrame: uid | homepage
		files = os.listdir(self.htmlpath)
		for expid in files:
			name = dic[expid][0]
			org = dic[expid][1]
			with open(self.htmlpath+expid) as html:
				soup = BeautifulSoup(html,'lxml')
				# [script.extract() for script in soup.findAll('script')]
				# [style.extract() for style in soup.findAll('style')]
			# homepage_url
				homeurl=None
				urls = []
				# print(' = ='*20)
				for t in soup.find_all('h3'):
					if not t.a:
						continue
					if not t.a['href']:
						continue
					url = t.a['href']
					if len(url) > 200:
						continue
					tmp = url.strip().strip('/').split('/')
					if len(tmp) < 4:
						continue
					if tmp[0] == '/':
						continue
					uname = tmp[-1]
					if uname[0] == '~':
						homeurl = url
						urls = []
						break
					urls.append(url)
				for url in urls:
					tmp = url.strip().strip('/').split('/')
					uorg = tmp[2].strip('w.')
					if type(org) is not str:
						org = str(org)
					if type(name) is not str:
						name = str(name)
					uname = tmp[-1]
					simi_name = Levenshtein.jaro(uname,name)
					simi_org = Levenshtein.jaro(uorg,org)
					if simi_org > 0.5 and simi_name > 0.8 and abs(len(uname) - len(name)) < 15:
						homeurl = url
						break
					if len(uname) < 3 or len(uorg) < 4:
						continue
					if simi_org < 0.2 and simi_name < 0.3:
						continue
					blk = ['baidu','wikipedia','researchgate','weibo.com','onlinebooks',
						'linkedin']
					st = [uorg.lower().find(x) for x in blk]
					if st.count(-1) < len(st):
						continue
					homeurl = url
					break
				self.homepage_url[expid] = homeurl
				print(homeurl)
				print('- + - '* 10)
			# email
				# emailid_regexp = re.compile(r'\S+\w+@\S+\w+')  
				# raw = soup.find(text=emailid_regexp) 
				# if not raw:
				# 	self.email[expid] = None
				# 	continue
				# r = re.search(r'[0-9a-zA-Z][0-9a-zA-Z._]+\w@[0-9a-zA-Z._]+\.[0-9a-zA-Z._]+\w',raw)
				# if not r:
				# 	self.email[expid] = None
				# 	continue
				# emailaddr=r.group()
				# self.email[expid] = emailaddr
				# print(emailaddr)  
		# #	# #
		dic = pd.Series(self.homepage_url)
		df = pd.DataFrame(dic)
		df.index.name = 'expert_id'
		df.columns = ['homepage_url']
		# df['homepage_url'] = df['homepage_url'].map(lambda url: '/'.join(url.split('/')[:-1]) if lower(url[-3:]) == 'pdf' else url)
		# df['email'] = pd.Series(self.email)
		return df
		
	def email_from_homeurl_file(self,ids,names):
		xyz = zip(ids,names)
		dic = {}
		for x in xyz:
			dic[x[0]] = x[1]

		files = os.listdir(self.htmlpath)
		# files=['53f43169dabfaedce54f8b96']
		for expid in files:
			print(expid + '- > !')
			try:
				with open(self.htmlpath+str(expid)) as html:
					soup = BeautifulSoup(html,'lxml')
					[script.extract() for script in soup.findAll('script')]
					[style.extract() for style in soup.findAll('style')]
					souptext = soup.get_text().replace('\\t','').replace('\\n','').replace('\\r','')
					# print(souptext)
				# email
					emailid_regexp = re.compile(r'\S+@\S+')  
					rs = re.findall(emailid_regexp,souptext)
					# print(rs)
					emailaddr = None
					if not rs:
						continue
					for raw in rs:
						r = re.search(r'[a-zA-Z][0-9a-zA-Z._-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+\w',raw,re.I)
						if not r:
							continue
						txt = r.group()
						if '.' not in txt:
							continue
						if len(txt.split('.')[-1]) > 4:
							continue
						if txt[:3] =='x9a': #encoding error fixed
							txt = txt[3:]
						if Levenshtein.jaro(str(dic[expid]),str(txt.split('@')[0])) > 0.9:
							emailaddr = txt
							break
						extract = ['office','root','support','service','example','admin','feedback',
						'webmaster','help','info@','ieeexplore','admissions','web@','lab@','group','academic']
						st = [txt.lower().find(x) for x in extract]
						if st.count(-1) < len(st):
							continue
						emailaddr = txt
						break
					self.email[expid] = emailaddr
					print('- - >'+str(expid)+'- >'+emailaddr)
			except Exception as e:
				print(e)
		email = pd.DataFrame(pd.Series(self.email))
		email.columns = ['email']
		email.index.name = 'expert_id'
		email = email.fillna(' ')
		return email


	def position_from_homeurl_file(self,positions):

		files = os.listdir(self.htmlpath)
		for expid in files:
			posit = []
			print(expid)
			with open(self.htmlpath+expid) as html:
				soup = BeautifulSoup(html,'lxml')
				[script.extract() for script in soup.findAll('script')]
				[style.extract() for style in soup.findAll('style')]
				txt = soup.get_text()
				# print(txt)
				for p in positions:
						if p in txt:
							posit.append(p)
			po = ';'.join(posit)
			print(po)
			self.position[expid] = po
		position = pd.DataFrame(pd.Series(self.position))
		position.columns=['position']
		position.index.name = 'expert_id'
		position = position.fillna(' ')
		return position

	def pic_from_homeurl_file(self,ids,urls):
		xyz = zip(ids,urls)
		dic = {}
		for m in xyz:
			item = m[1]
			key  = m[0]
			dic[key] = item
		com = set('jpg|JPG|jpeg|JPEG|gif|GIF|png|PNG'.split('|'))
		files = os.listdir(self.htmlpath)
		for expid in files:
			pic = ''
			with open(self.htmlpath + expid) as html:
				soup = BeautifulSoup(html,'lxml')
				imgs = soup.find_all('img')
				for pc in imgs:
					if not pc:	continue
					# print(pc)
					if not pc.get('src'):	continue
					m = pc['src'].strip()
					if len(m) < 4:
						continue
					if '.' not in m: continue
					sp  = m.split('.')
					# ilegal file type, not a photo !
					dot = sp[-1]
					if dot not in com:
						continue
					w = pc.get('width')
					if type(w)== str:
						if len(w) > 0:
							if w[-1] == '%': 
								pass
							else:
								w =re.findall("\d+",w)
								if len(w) > 0:
									w = w[0]
									if int(w) < 50: continue
					w = pc.get('height')
					if type(w) == str :
						if len(w) > 0:
							if w[-1] == '%': 
								pass
							else:
								w =re.findall("\d+",w)
								if len(w) > 0:
									w = w[0]
									if int(w) < 50:continue
					# ilegal name, surely not person photo
					picname = sp[-2]
					extract = ['logo','lbl','load','nb_NO','icon','search',
						'tube','cloud','title','cover','close','out']
					s = [picname.lower().find(x) for x in extract]
					if s.count(-1) < len(s):
						continue
					print(m)
					# print(dic[expid])
					pic = urljoin(dic[expid],m)
					break
			print(dic[expid])
			self.person_photo[expid] = pic
			print(' - * - '*10)
			print(pic)
			print('= = ='*20)
		pic = pd.DataFrame(pd.Series(self.person_photo))
		pic.columns = ['person_photo']
		pic.index.name = 'expert_id'
		pic = pic.fillna(' ')
		return pic


					



