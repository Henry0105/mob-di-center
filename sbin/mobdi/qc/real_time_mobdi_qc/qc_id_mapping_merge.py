from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
from pyspark import SparkContext

import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def qc_id_mapping_merge(spark):
    table=sys.argv[1]
    table_par=sys.argv[2]
    last_table_par=sys.argv[3]

    cnt_today=spark.sql("select count(1) from %s where day='%s'" % (table, table_par)).collect()[0][0]
    cnt_yes=spark.sql("select count(1) from %s where day='%s'" % (table, last_table_par)).collect()[0][0]

    rate = (float)(cnt_today-cnt_yes)/(cnt_yes+1)

    if rate<0 :
        print("less than yesterday")
        sys.exit(1)
    elif  rate>0.1:
        print("much than 10%")
        sys.exit(2)
    else:
        print("qc success")



def main():
    spark = SparkSession.builder \
        .appName('qc_id_mapping_%s_%s' % (sys.argv[1], sys.argv[2])) \
        .config("spark.logConf", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    qc_id_mapping_merge(spark)

    sc.stop()
    spark.stop()


if __name__ == '__main__':
    main()
