from  pyspark.sql import SQLContext,SparkSession,Row
from  pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import *
from pyspark import SparkContext

import pandas as pd
import numpy as np
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def read_profile(spark):
  excel_file="new_profiles.xlsx"
  pd_df = pd.read_excel(excel_file,header=0,dtype={'profile_id':str,'source_type':str,'timewindow':str,'computer_type':str,'percent':str,'has_apppkg_mapping':str,'version':str}).fillna('')
  #pd_df = pd.read_excel(excel_file,header=0,encode='utf-8').fillna('')
  #pd_df = pd.read_excel(excel_file,header=0,dtype={'source_type':str,'version':str}).fillna('')
  #pd_df =pd_df.replace(np.NaN, '').dropna()
  pd_df =pd_df.dropna(axis=0).fillna('')
  
  print(pd_df.tail(100))
  schema = StructType( \
              [StructField("profile_id", StringType(), True), \
               StructField("profile_name", StringType(), True), \
               StructField("source_type", StringType(), True), \
               StructField("cate_id", StringType(), True), \
               StructField("cate", StringType(), True), \
               StructField("timewindow", StringType(), True), \
               StructField("computer_type", StringType(), True), \
               StructField("filter", StringType(), True), \
               StructField("has_apppkg_mapping", StringType(), True), \
               StructField("percent", StringType(), True), \
               StructField("version", StringType(), True) \
              ])
  df = spark.createDataFrame(pd_df,schema=schema).filter("profile_id!='nan'")
  df=df.withColumn("percent",when(df.percent=='nan',"").otherwise(df.percent)).registerTempTable("profile")
  spark.sql("""
            insert overwrite table dim_sdk_mapping.dim_online_profile_mapping_v3 partition(version='20190513')
               
               select profile_id,profile_name,source_type,filter,has_apppkg_mapping,cate_id,cate,timewindow,computer_type,percent from profile 
             """)

def main():
  spark =SparkSession.builder\
      .appName('gen_profilemapping_from_excel')\
      .config("spark.logConf", "true")\
      .enableHiveSupport()\
      .getOrCreate() 
  
  sc=spark.sparkContext
  sc.setLogLevel("WARN")

  read_profile(spark)

  sc.stop()
  spark.stop()

if __name__=='__main__':
  main()
