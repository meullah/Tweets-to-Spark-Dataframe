#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import tweepy
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import datetime


# In[2]:


# create spark context & Session 
sc = pyspark.SparkContext()
spark = spark = SparkSession.builder.getOrCreate()


# In[4]:


# API credentials here
consumer_key = 'BwfynXQPFAtO7FDB6RshgdJi1'
consumer_secret = 'CfQNAYRO59mpOHnd0E14q04IgrOLNTW18VSXZDFqFi2Y6ql2Ni'
access_token = '2178398821-jLyK8eTM8dTZ0sAy5lrlEy9gzgD8TMyFqQVEcpj'
access_token_secret = 'cmDeuzWdxIRjmcY6FZ6l3CKEk4g3YFlsmyKDu0DOoxZjy'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)


# In[14]:


HashTag = input("Enter Hashtag : ")
num = int(input("Number of days from today : "))
StartDate = datetime.datetime.today().date() - datetime.timedelta(days=num)


# In[15]:


tweets = tweepy.Cursor(api.search,q=HashTag,count=20,lang="en",since=StartDate, tweet_mode='extended').items(20)


# In[16]:


columns = ["created_at","text", "country"]
df=[]


# In[17]:


for tweet in tweets:
    df.append((tweet.created_at, tweet.full_text.encode('utf-8'), tweet.user.location))


# In[18]:


df = spark.createDataFrame(df, columns)


# In[20]:


grouped_df = df.groupBy("country").count().alias("Tweets Number")
grouped_df.show()


# In[ ]:


aaa.repartition(1).write.csv("tweetsAsPerCountry.csv")

