# import crawler

import glob 
import PM
import os


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
import task3
task3.transform_src()
task3.remove_dup_ref()

# # paper_info.csv generated, then we may try to build db table for further use

# # build_db_tables--------------------------------------------------------------->

import build_db_tables
build_db_tables.create_db()
build_db_tables.create_tab_write()
build_db_tables.create_tab_reference()
build_db_tables.create_tab_author()
build_db_tables.create_author_paper()

# # # switch back for extracting features for author: class task3_feature():--------->
tf = task3.task3_feature()

tf.author_paper_place()
tf.author_place_rank()
tf.author_year_count()
tf.author_paper_publish()
tf.author_paper_coauthor()
tf.author_paper_cited()
tf.author_cited_count()


# # # paper rank for calculating the value of an author's work----------------------->
task3.paper_rank()

build_db_tables.create_paper_pr_cc_year_pub()

task3.author_rank()
task3.publish_rank()

# # extract feauture of author's work quality --------------------------------------->
tf.maiden_work()
tf.guamin_work()
tf.place_1_work_citation()
tf.place_1_work_3y_citation()
tf.place_all_work_citation()
tf.place_all_before_2000_citation()
tf.place_all_in_2000_2010citation()
tf.place_all_in_2010_2014citation()

# # merge features for PM.author_description_path --------------------------------->
# 
task3.merge_feature()

# #start the most important part: feature engineering and model training:--------->

import task3model

task3model.prepare_training()

task3.sub_task3_file()
os.system('./filter.sh') # hander the format of ", 
# - > ../data/submission/task3.csv
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  # # # # # # 
import task2

task2.build_labelcsv()
task2.build_traincsv()
task2.build_validcsv()

build_db_tables.create_interest_table()

task2.create_publish_interest()
task2.publish_2_topic()

task2.author_2_publish_2_topic()
task2.sub_task2_file()
# - > ../data/submission/task2.csv
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

import task1

task1.build_validCSV()
task1.build_trainCSV()


task1.crawl_valid_srp_url()

# task1.crawl_train_srp_url()

# #search result page - - > homepage url - -> homepage files - > email, gender, ...

task1.raw_build_homepage_url() # search result page -> homepage url list

task1.crawl_valid_home_page() # crawl homepage files

task1.parse_homepage_for_email() # email
task1.parse_homeurl_for_location() # location
task1.parse_homepage_for_position() # position
task1.parse_homepage_for_personphoto() # person_photo
task1.parse_name_for_gender() # gender

task1.task1_result()
# - > ../data/submission/task1.csv
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# generate the final result : temp.txt

import final

final.submission()



