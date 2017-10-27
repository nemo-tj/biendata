import os
import shutil
import csv,sqlite3

class util(object):
	"""docstring for util"""
	def __init__(self):

		self.data_p = '../data/'
		if  os.path.exists(self.data_p):
			pass
			# print('%s already exists ....'%self.data_p)
		else:
			print('create path - - > %s ....'%self.data_p)
			os.mkdir(self.data_p)

	def mk_dir(self,path):
		if  os.path.exists(path):
			pass
			# print('%s already exists .....'%path)
		else:
			os.mkdir(path)
			print('create path - - > %s ....'%path)

	def rm_dir(self,path):
		if os.path.exists(path):
			shutil.rmtree(path)

ut = util()
path = "../"
data_path = path+"data/"
tmp_path = path+'tmp/'
test_path = path +'scholar_test_final/'

ut.mk_dir(data_path)
ut.mk_dir(tmp_path)
submission = data_path+'submission/'
ut.mk_dir(submission)
sub_task1 = submission+'task1.csv'
sub_task2 = submission+'task2.csv'
sub_task3 = submission+'task3.csv'

sub_txt = submission+'temp.txt'

task3_path = path+"tasks/task3/"
ut.mk_dir(task3_path)
task3_papers_txt = task3_path+"papers.txt"
task3_train_csv = task3_path+'train.csv'
# task3_validation_csv = task3_path+'validation.csv'
task3_validation_csv = test_path + 'task3_test_final.csv'

task2_path = path+"tasks/task2/"
ut.mk_dir(task2_path)
task2_labels_file = task2_path+'labels.txt'
task2_training_file = task2_path+'training.txt'
# task2_validation_file = task2_path+ 'validation.txt'
task2_validation_file = test_path+ 'task2_test_final.txt' 

task1_path = path+'tasks/task1/'
ut.mk_dir(task1_path)
task1_training_txt = task1_path+'training.txt'
# task1_validation_txt = task1_path+ 'validation.txt'
task1_validation_txt = test_path+ 'task1_test_final.txt'

# -----------db---------------
db = data_path+"final.db"
# db = data_path+"comp.db"
# db = data_path+'kick.db'

# ------------task3-----------------------------

data_path_task3 = data_path+"task3/"
ut.mk_dir(data_path_task3)
paper_info = data_path_task3+"paper_info.csv"
paper_rank = data_path_task3+'paper_rank.csv'
# publish_rank = data_path_task3+'publishrank.csv'

author_feature_path = data_path_task3+"feature/"
ut.mk_dir(author_feature_path)

# task3_author_feature_path= data_path + "authorfeature/"
# task3_year_related_path = task3_author_feature_path+ 'yearbased/'
# task3_year_irregular_path = task3_author_feature_path +'yearnot/'
# ut.mk_dir(task3_author_feature_path)
# ut.mk_dir(task3_year_irregular_path)
# ut.mk_dir(task3_year_related_path)

author_description_path = data_path_task3+'authordesc/'
ut.mk_dir(author_description_path)
author_train_tabl = author_description_path+'trainset.csv'
author_validation_tabl = author_description_path+'validationset.csv'
# author_train_tabl = author_description_path+'t_trainset.csv'
# author_validation_tabl = author_description_path+'v_validationset.csv'


author_trainmodel_path = data_path_task3+'modeltrain/'
ut.mk_dir(author_trainmodel_path)
task3_model = author_trainmodel_path+'xgb.model'
mean_std_params = author_description_path +'stdmean.csv'
# feature select columns:
feautur_col = author_trainmodel_path+'featurecol.csv'
model_train_set = author_trainmodel_path +'m_trainset.csv'
model_valid_set = author_trainmodel_path +'m_validset.csv'
train_set_t3 = author_trainmodel_path+'train_set.csv'
valid_set_t3 = author_trainmodel_path+'valid_set.csv'
 

author_pred_path = data_path_task3 +'preds/'
ut.mk_dir(author_pred_path)

fcol = ['cited_sum_times',
'sum_citation_10000',
'author_pub_max_times',
'sum_pr_10000',
'pr_sum_value',
'author_pub_mean_times',
'author_pub_sum_coauthor',
'author_pub_max_coauthor',
'max_citation_2013',
'cited_max_times',
'median_pr_10000',
'pr_median_value',
'max_citation_2009',
'cited_mean_times',
'under_place_20_num',
'median_pr_2009',
'max_pr_2009',
'sum_citation_1111',
'sum_citation_2009',
'sum_citation_1999',
'max_citation_10000',
'cited_std_times',
'under_place_8_num',
'median_citation_2009',
'under_place_10_num',
'sum_pr_2009',
'pr_std_value',
'max_pr_2013',
'pp_3_num',
'author_pub_mean_coauthor',
'pr_max_value',
'pubsumcited_sum',
'max_citation_1111',
'pp_last_place_count']

# feature_columns = ['pr_sum_value', 'pr_mean_value', 'pr_max_value', 'pr_std_value',
#        'cited_sum_times', 'cited_max_times', 'cited_median_times',
#        'cited_std_times', 'author_pub_max_coauthor',
#        'author_pub_mean_coauthor', 'author_pub_min_coauthor',
#        'author_pub_sum_coauthor', 'after_2013_num', 'after_2012_num',
#        'after_2011_num', 'after_2009_num', 'after_2008_num', 'before_2009_num',
#        'before_2003_num', 'before_1991_num', 'before_1982_num',
#        'author_pub_kinds', 'author_pub_max_times', 'author_pub_mean_times',
#        'pubmaxcited_max', 'pubmaxcited_mean', 'pubmaxcited_sum',
#        'pubmaxcited_std', 'pubsumcited_max', 'pubsumcited_mean',
#        'pubsumcited_sum', 'pubsumcited_std', 'top_place_1_num',
#        'top_place_2_num', 'top_place_3_num', 'under_place_4_num',
#        'under_place_8_num', 'under_place_20_num',
#        'gm_last_times',
#        'f3_maiden_time', 'pp_ranking', 'paper_citations', 'citation_sums', 
#        'citation_mean', 'citation_median', 'citation_max', 'pubpaper_nums'
#        ]


# ------------task2-----------------------------
data_path_task2 = data_path+"task2/"
ut.mk_dir(data_path_task2)

task2_labels_csv =data_path_task2+'labels.csv'
task2_training_csv = data_path_task2+'training.csv'
task2_validation_csv = data_path_task2 +'validation.csv'

dic_path = data_path_task2 +'dictory/'
ut.mk_dir(dic_path)
publish_topic = dic_path+'pubtopic.csv'
author_topic = dic_path +'authortopic.csv'
task2_valid_pred  = dic_path +'validpred.csv'

task2_preds_path = data_path_task2+'preds/'
ut.mk_dir(task2_preds_path)
task2_target = task2_preds_path+'target.csv'


# ----------------- task1 --------------------------------------------
data_path_task1 = data_path +'task1/'
ut.mk_dir(data_path_task1)
task1_training_csv = data_path_task1+'training.csv'
task1_validation_csv = data_path_task1 +'validation.csv'

task1_ans_path = data_path_task1+'ans/'
ut.mk_dir(task1_ans_path)
task1_homeurl_csv = task1_ans_path+'homeurl.csv'
task1_first_is_home_csv = task1_ans_path + 'firstIsHome.csv'
task1_email_csv = task1_ans_path+'email.csv'
task1_location_csv = task1_ans_path+'location.csv'
task1_gender_csv = task1_ans_path+'gender.csv'
task1_personpic_csv = task1_ans_path+'personpic.csv'
task1_position_csv = task1_ans_path+'position.csv'


task1_valid_home_path = data_path_task1+'validhomepath/'
ut.mk_dir(task1_valid_home_path)
task1_validgoogle_path = data_path_task1+'GoogleListValid/'
ut.mk_dir(task1_validgoogle_path)
task1_traingoogle_path = data_path_task1+'GoogleListTrain/'
ut.mk_dir(task1_traingoogle_path)



task1_result_path = data_path_task1+'prediction/'
ut.mk_dir(task1_result_path)



