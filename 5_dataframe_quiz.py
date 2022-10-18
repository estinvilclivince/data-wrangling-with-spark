#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with DataFrames Coding Quiz
# 
# Use this Jupyter notebook to find the answers to the quiz in the previous section. There is an answer key in the next part of the lesson.

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc,col
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
import datetime
import numpy as np
import pandas as pd
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
# TODOS: 
# 1) import any other libraries you might need
# 2) instantiate a Spark session 
spark = SparkSession.builder.appName("dataframe quiz").getOrCreate()
# 3) read in the data set located at the path "data/sparkify_log_small.json"
user_log = spark.read.json("data/sparkify_log_small.json")
# 4) write code to answer the quiz questions 
from pyspark.sql.window import Window


# In[3]:


user_log.printSchema()


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# In[ ]:


visited_page = user_log.filter(user_log.userId=="").select("page").collect()


# In[ ]:


set(user_log.select('page').dropDuplicates().collect()) - set(visited_page)


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# In[2]:


# TODO: use this space to explore the behavior of the user with an empty string


# In[ ]:





# # Question 3
# 
# How many female users do we have in the data set?

# In[3]:


# TODO: write your code to answer question 3


# In[ ]:


user_log.filter(user_log.gender=="F").select(["userId","gender"]).dropDuplicates().count()


# # Question 4
# 
# How many songs were played from the most played artist?

# In[ ]:


# TODO: write your code to answer question 4


# In[ ]:


user_log.filter(user_log.artist!="null").groupBy(user_log.artist).count().orderBy(desc("count")).show(1)


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# In[ ]:


# TODO: write your code to answer question 5


# In[ ]:


function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

user_window = Window     .partitionBy('userID')     .orderBy(desc('ts'))     .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home'))     .select('userID', 'page', 'ts')     .withColumn('homevisit', function(col('page')))     .withColumn('period', Fsum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong'))     .groupBy('userID', 'period')     .agg({'period':'count'})     .agg({'count(period)':'avg'}).show()


# In[ ]:


filter_home = udf(lambda is_home : int(is_home == "Home"), IntegerType())


# In[ ]:


user_window = Window.partitionBy('userID').orderBy(desc('ts')).rangeBetween(Window.unboundedPreceding,0)


# In[ ]:


cum_sum = user_log.filter((user_log.page=="NextSong")|(user_log.page=="Home")).select('userID','page','ts').withColumn('homevisit',filter_home(col('page')))    .withColumn('period',Fsum('homevisit').over(user_window))


# In[ ]:


cum_sum.show()


# In[ ]:


cum_sum.filter((cum_sum.page=="NextSong")).groupBy('userID','period').agg({'period':'count'}).agg({'count(period)':'avg'}).show()

