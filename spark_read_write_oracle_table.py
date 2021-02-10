from __future__ import print_function
from __future__ import division
from pyspark.sql import SparkSession
import datetime
import uuid
from datetime import date , timedelta,time
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import random
from pyspark.sql import SparkSession


import time;
ts = time.time()
print(ts)


def getCurrDate():
       currentDate=datetime.datetime.now().strftime("%Y%m%d")
       filesDate=datetime.datetime.strptime(currentDate, '%Y%m%d')-timedelta(days=int(3))
       sdate=str(filesDate).split(' ')
       sd = sdate[0].split('-')
       sds=sd[0]+sd[1]+sd[2]
       return str(currentDate)




def getSparkSession():
    if ("sparksession" is not globals()):
            globals()["sparksession"] = SparkSession \
                .builder \
                .appName("vm reports") \
                .config("spark.driver.extraClassPath", "/opt/mapr/lib/ojdbc6.jar")\
                .getOrCreate()
    return globals()["sparksession"]


query3 = "(select  /*+parallel*/ cast(TID as varchar(40)) SRC_CD_TID,cast(AGENT_MSISDN as varchar(20)) SRC_NR_AGNT_MSISDN,cast(round(CUSTOMER_MSISDN,0) as varchar(50)) SRC_NR_CSTMR_MSISDN,cast(CATEGORY as varchar(50)) SRC_DS_CATEGORY,TO_CHAR(TRX_DATE,'YYYYMMDD') SRC_DT_TRX,cast(ACTIVE_STATUS as varchar(50)) SRC_FL_STATUS,DESCRIPTION SRC_DS,SUB_CATEGORY SRC_DS_SUB_CATEGRY,LOCATION SRC_DS_LOCATION,SALES_AREA SRC_DS_SALES_AREA,ACCOUNT_NUMBER X_ACCT_NO,SERVICE_PLAN SRC_CD_SRVC_PLAN,TYPE_OF_BUNDLE SRC_CD_BUNDLE_TYPE,OUTLETNAME SRC_DS_OUTLET,AGENT_NAME SRC_DS_AGNT_NAME,IMEI SRC_CD_IMEI,TOTALS SRC_VL_TTL, sysdate TIMESTAMP from VW_SUBS) VW_SUBS"

query = "(SELECT TO_CHAR(SRC_DT_TRX,'YYYYMMDD') ID_DATE, SRC_DS_LOCATION,round(count(SRC_NR_CSTMR_MSISDN),0) count FROM IC_STAGE.SRC_880_TRX_DTLS WHERE TO_CHAR(SRC_DT_TRX,'YYYYMMDD') = '20190317' group by TO_CHAR(SRC_DT_TRX,'YYYYMMDD') , SRC_DS_LOCATION)  SRC_880_TRX_DTLS" 


query2 = "(select /*+parallel*/ TO_CHAR(SRC_DT_TRX,'YYYYMMDD') ID_DATE,SRC_DS_CATEGORY DS_CATEGORY,SRC_DS_LOCATION DS_LOCATION,COUNT(*) COUNT from ic_stage.SRC_880_TRX_DTLS where  TO_CHAR(SRC_DT_TRX,'YYYYMMDD') >=20190201 and  SRC_DS_CATEGORY='Blaze' AND SRC_NR_CSTMR_MSISDN IS NOT NULL GROUP BY SRC_DS_LOCATION,TO_CHAR(SRC_DT_TRX,'YYYYMMDD'),SRC_DS_CATEGORY,SRC_DS_LOCATION) SRC_880_TRX_DTLS"



def getReadSource():
        df= getSparkSession().read.format("jdbc") \
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
		.option("dbtable",query3) \
                .option("user", "") \
                .option("password","") \
		.load()
        return df



def getLoadTempTable():
	getSparkSession().catalog.dropGlobalTempView("VW_SUBS")
	getReadSource().createGlobalTempView("VW_SUBS")
	df=getSparkSession().sql("select  /*+parallel*/  cast(round(nvl(cast(SRC_CD_TID as int),0),0) as varchar(50)) SRC_CD_TID,cast(round(nvl(SRC_NR_AGNT_MSISDN,0),0) AS varchar(100))  SRC_NR_AGNT_MSISDN,cast(round(nvl(SRC_NR_CSTMR_MSISDN,0),0) AS varchar(100)) SRC_NR_CSTMR_MSISDN,cast(round(nvl(SRC_DS_CATEGORY,'0'),0) as varchar(100)) SRC_DS_CATEGORY,SRC_DT_TRX  SRC_DT_TRX ,cast(round(nvl(SRC_FL_STATUS,0),0) as varchar(100)) SRC_FL_STATUS, CAST(SRC_DS AS VARCHAR(50)) SRC_DS,CAST(round(nvl(SRC_DS_SUB_CATEGRY,0),0) AS VARCHAR(50)) AS SRC_DS_SUB_CATEGRY,CAST(SRC_DS_LOCATION AS VARCHAR(50)) SRC_DS_LOCATION,CAST(SRC_DS_SALES_AREA AS VARCHAR(100)) AS SRC_DS_SALES_AREA,cast(X_ACCT_NO as varchar(50)) X_ACCT_NO,CAST(SRC_CD_SRVC_PLAN AS VARCHAR(50)) SRC_CD_SRVC_PLAN,CAST(SRC_CD_BUNDLE_TYPE AS VARCHAR(50)) AS SRC_CD_BUNDLE_TYPE,CAST(SRC_DS_OUTLET AS VARCHAR(100)) AS SRC_DS_OUTLET ,CAST(SRC_DS_AGNT_NAME AS VARCHAR(100)) AS SRC_DS_AGNT_NAME,cast(SRC_CD_IMEI as varchar(50)) SRC_CD_IMEI,cast(SRC_VL_TTL as varchar(50)) SRC_VL_TTL from global_temp.VW_880_SUBSCRIPTIONS")
	return df 


def partitionTable():
        getSparkSession().read.format("jdbc") \
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
		.option("dbtable",query4) \
		.option("user", "") \
		.option("password","") 

 #    getLoadTempTable().write.format("jdbc") \

def writeTableSource():
	getLoadTempTable().write.format("jdbc") \
		.mode('append') \
		.option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","<table_name>") \
                .option("user", "") \
                .option("password","") \
                .save()




def getReadTable():
	df= getSparkSession().read.format("jdbc") \
		.option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
		.option("driver","oracle.jdbc.OracleDriver") \
		.option("dbtable",query) \
        	.option("user", "") \
		.option("password","") \
		.load()
	return df


def writeTable():
        getReadTable().write.format("jdbc") \
                .mode('append')\
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","table_name") \
                .option("user", "") \
                .option("password","") \
                .save()



def  getReadTable2():
	df2= getSparkSession().read.format("jdbc") \
        	.option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
	        .option("driver","oracle.jdbc.OracleDriver") \
        	.option("dbtable",query2) \
	        .option("user", "") \
        	.option("password","") \
	        .load()
	return df2

def writeTable2():
        getReadTable2().write.format("jdbc") \
                .mode('append')\
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","table_name") \
                .option("user", "") \
                .option("password","") \
                .save()


dataSchema = StructType()\
        .add("id_date", "integer")\
        .add("SRC_DS_LOCATION", "string")\
        .add("COUNT", "integer")\




def loadData():
        getReadTable().write.format("jdbc") \
                .option("url","jdbc:oracle:thin:@<host>:<port>/<schema>") \
                .option("driver","oracle.jdbc.OracleDriver") \
                .option("dbtable","table_name") \
                .option("user", "") \
                .option("password","") \
		.save()



getCurrDate()
