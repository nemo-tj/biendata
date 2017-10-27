# -*- coding:utf-8 -*-

import os
import numpy as np
import pandas as pd
import PM


def task(file,i):
	taski = PM.submission + 'task%d.csv'%(i)
	file.write("<task%d>"%(i))
	file.write("\n")
	with open(taski) as ti:
		file.write(ti.read())
	
	file.write("</task%d>"%(i))
	file.write("\n")

# sed -i 's/"\t/\t/g' task3.csv 


temp = PM.sub_txt
def submission():
	with open(temp,'w') as file:
		for i in range(1,4):
			task(file,i)
	file.close()


