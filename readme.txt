----------------------
	（1）模型简介：
----------------------
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
task3
1. 处理 task3， 建立 paper_info 表格 - - > 建立数据库，方便后续数据提取
2. 提取 学者的特征和筛选，task3.task3_feature() ... - - > 生成训练集和测试集
3. 利用 xgboost 进行训练和预测

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
task2
1. 处理 task2，建立学者的 兴趣表 + 期刊的主题表
2. 根据学者论文在不同期刊上的分布，计算学者对不同兴趣的侧重度
	（1）期刊有相应的主题；
	（2）学者发表的论文分布在不同的期刊上；
流程 :
 - > 根据训练集中的数据，计算不同期刊的主题 
 - > 根据期刊的主题 + 学者在不同期刊上发表论文情况 
 - > 学者的兴趣 ！

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
task1
 1. 处理 task1,爬取 google_search_result 的网页
 2. 从 google_search_result 中判断 主页的链接 homeurl
 3. 爬取主页，并从主页提取 email,location,position,personphoto,gender

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

----------------------
	（2）运行流程：
----------------------
- > 运行环境是 Anaconda3-4.4.0, 
- > 额外依赖包：xgboost, python-graph-core,pygraph,sqlite3
	pip install xgboost
	pip install pygraph
	pip install python-graph-core
	pip install sqlite3
- > python 版本 Python 3.6.1 :: Anaconda 4.4.0 (x86_64)
- > 项目文件结构如图：project_structure.jpg

1. 将task1的原始数据解压后，放在 tasks/task1/ 路径下
2. 将task2的原始数据解压后，放在 tasks/task2/ 路径下
3. 将task3的原始数据解压后，放在 tasks/task3/ 路径下
4. 将最终的测试集解压后的文件，放在 scholar_test_final 路径下
5. 进入source 路径下，运行 build.sh 脚本（该脚本将运行 run.py 文件)

- > 最终结果在 data/submission/temp.txt
- > task1.csv,task2.csv,task3.csv 分别为task 1，2，3的结果

----------------------
	（3）说明：
----------------------
1. task1 需要爬取数据，所以需要花一定的时间；
2. 整个项目的运行过程由 run.py 统一调度；
3. 中间数据文件的路径在 PM.py 中配置
4. filter.sh 是用于消除 任务3结果task3.csv中 学者姓名中有多余的 双引号
   该脚本运行于 linux 系统，调用sed -i 命令。
   可能与平台相关，如果失效，需要手工删除多余的引号，一共有 7 行，eg:
   "Arthur ""Buck"" Nimz"	16 - > Arthur "Buck" Nimz	16
5. final.py 用于合并 三个人任务的结果 task1.csv, task2.csv,task3.csv
5. build.sh 脚本用于调度 run.py



