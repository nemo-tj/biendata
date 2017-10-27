
import PM
import pandas as pd


# ==============================================================================================
from xgboost import plot_importance
from matplotlib import pyplot as plt

from sklearn import preprocessing
from sklearn.feature_selection import VarianceThreshold,RFE,RFECV,SelectFromModel
from sklearn.feature_selection import SelectKBest,f_regression,mutual_info_regression

import xgboost as xgb
from sklearn.model_selection import train_test_split,StratifiedKFold

from sklearn.linear_model import LinearRegression,LassoCV,SGDRegressor

from sklearn.svm import SVR

from sklearn import ensemble
from sklearn.utils import shuffle
from sklearn.metrics import mean_squared_error,mean_absolute_error

import numpy as np


def prepare_training():
	print('training ...........')
	X_Y = pd.read_csv(PM.author_train_tabl,header = 0,sep=u'|',index_col='author_name').fillna(00)
	y_train = X_Y.CITATION
	x_train = X_Y.drop('CITATION',axis=1)
	x_train = x_train.loc[:,PM.fcol]
	print('shape --> %s'% str(x_train.shape))

	# finish feature selection!
	model = simple_train(x_train,y_train)
	# finish model training 
	# return 
	print('start predicting .......')
	val = pd.read_csv(PM.author_validation_tabl,header = 0,sep=u'|',index_col='author_name').fillna(0)
	val = val.loc[:,PM.fcol]
	x_val = val

	# ok, do predicting.........
	T = xgb.DMatrix(x_val)
	preds = model.predict(T)
	preds[preds < 0] = 0
	# print(pre.columns)
	column = ['authorname','citation']
	result = pd.DataFrame(columns = column)

	result['authorname']= val.index
	result['citation'] = preds.astype('int64');
	print('prediction generated, write to csv file....')
	result.to_csv(PM.author_pred_path+'task3.csv',sep='\t',index = False,header =1)
	print(result[:5])

def evalerror(preds,dtrain):
	labels = dtrain.get_label()
	preds[preds < 0] = 0
	error = [0 if x < 1 and y < 1 else abs(x-y)/(max(x,y)*1.0) for x, y in zip(preds,labels)]
	return 'error',float(sum(error))/len(error)

def simple_train(x_train,y_train):
	xy = pd.concat([x_train,y_train],axis=1)
	train_sample,val_sample = train_test_split(xy, test_size = 0.2, random_state= 10)

	y_train = train_sample.CITATION
	x_train = train_sample.drop(['CITATION'],axis=1)
	xgb_train = xgb.DMatrix(x_train, label=y_train)

	y_val = val_sample.CITATION
	x_val = val_sample.drop(['CITATION'],axis=1)
	xgb_val = xgb.DMatrix(x_val, label=y_val)

	# params value
	params={
	# general parameters
		'booster':'gbtree',
		'nthread':4,# cpu 线程数
	# booster parameters
		'subsample':0.7, # 随机采样训练样本
		'colsample_bytree':0.7, # 生成树时进行的列采样
		'min_child_weight':13, 
		
		'lambda':1,
		'alpha':3,
		# 这个参数默认是 1，是每个叶子里面 h 的和至少是多少，对正负样本不均衡时的 0-1 分类而言
		#，假设 h 在 0.01 附近，min_child_weight 为 1 意味着叶子节点中最少需要包含 100 个样本。
		#这个参数非常影响结果，控制叶子节点中二阶导的和的最小值，该参数值越小，越容易 overfitting。 
		'silent':0 ,#设置成1则没有运行信息输出，最好是设置为0.
		'eta': 0.03, # 如同学习率
		'seed':1000,
		'gamma':20,
		'max_depth':6,

	# task parameters
		'objective': 'reg:linear', #linear regression
		'eval_metric': 'mae'
	}
	plst = list(params.items())
	num_rounds = 1000 # 迭代次数
	watchlist = [(xgb_train, 'train'),(xgb_val, 'val')]
		#训练模型并保存
	# early_stopping_rounds 当设置的迭代次数较大时，early_stopping_rounds 可在一定的迭代次数内准确率没有提升就停止训练
	model = xgb.train(plst, xgb_train, num_rounds, watchlist,early_stopping_rounds=100)#,feval =evalerror)
	# model.save_model(PM.author_description_path+'xgb_iteration_1.model') # 用于存储训练出的模型
	print ("best best_ntree_limit--->"+ str(model.best_ntree_limit))
	preds = model.predict(xgb_val)
	preds[preds < 0] = 0
	labels = y_val
	error = [0 if x < 1 and y < 1 else abs(x-y)/(max(x,y)*1.0) for x, y in zip(preds,labels)]
	print(float(sum(error))/len(error))
	print('- 8 - '*20)
	# plot_importance(model)
	# plt.show()
	# ps = pd.Series(model.get_fscore()).sort_values(ascending=False)
	# df = pd.DataFrame(ps)
	# df.to_csv(PM.submission+'f.csv',sep='\t',header=1,index=True)
	# print(ps)
	print('--'*40)
	return model

