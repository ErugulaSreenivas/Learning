# Learning
m_sc_1
*******************************
Combined Data input query:
select a.OBJECT_ID,a.COLUMN_NAME,a.DATA_TYPE  from (select  DBS.NAME AS OWNER,DBS.DB_ID,TBLS.TBL_NAME as OBJECT_NAME,TBLS.TBL_ID as OBJECT_ID,TBLS.TBL_TYPE as OBJECT_TYPE,COLUMNS_V2.COLUMN_NAME, COLUMNS_V2.COMMENT as COLUMN_DESCRIPTION, COLUMNS_V2.TYPE_NAME AS DATA_TYPE from DBS JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID JOIN SDS ON TBLS.SD_ID = SDS.SD_ID JOIN COLUMNS_V2 ON COLUMNS_V2.CD_ID = SDS.CD_ID and TBLS.TBL_TYPE NOT LIKE 'VIRTUAL_VIEW')a order by a.OBJECT_ID,a.COLUMN_NAME,a.DATA_TYPE

Hive:
select a.NAME as DB_NAME,b.TBL_NAME,concat(a.NAME,'@',b.TBL_NAME) as DB_TBL,b.CREATE_TIME from DBS a join TBLS b on a.DB_ID = b.DB_ID

Dum_query:
select a.database_name as DB_NAME,a.tablename as TBL_NAME,concat(a.database_name,'@',a.tablename) as DB_TBL, count(a.ACTION) as Total_Count  from db_bdcs.v_dum360_ccb a where tablename not in('N/A', '') and platform ='Cloudera' and cluster ='MT-DISCOVERY'  and access_allowed_flag = 'Y' group by a.database_name,a.tablename order by Total_Count desc

Database details:
select a.OWNER, a.OBJECT_ID, a.OBJECT_NAME, a.OBJECT_TYPE, count(a.COLUMN_NAME) as COLUMN_COUNT from (select  DBS.NAME AS OWNER,DBS.DB_ID,TBLS.TBL_NAME as OBJECT_NAME,TBLS.TBL_ID as OBJECT_ID,TBLS.TBL_TYPE as OBJECT_TYPE,COLUMNS_V2.COLUMN_NAME, COLUMNS_V2.COMMENT as COLUMN_DESCRIPTION, COLUMNS_V2.TYPE_NAME AS DATA_TYPE from DBS JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID JOIN SDS ON TBLS.SD_ID = SDS.SD_ID JOIN COLUMNS_V2 ON COLUMNS_V2.CD_ID = SDS.CD_ID and TBLS.TBL_TYPE NOT LIKE 'VIRTUAL_VIEW')a group by a.OWNER, a.OBJECT_ID, a.OBJECT_NAME, a.OBJECT_TYPE order by COLUMN_COUNT desc

***********************
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import warnings
from collections import namedtuple
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark import since
from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.functions import trim
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, StructType
from pyspark.sql import functions as f
from pyspark.sql import types as t 

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

def cleancolumns(x):
    firstbrace=x.find('(')
    lastbrace=x.rfind(')')
    cleaneddata=x[firstbrace+1:lastbrace]
    cleanval=cleaneddata.split(',',2)
    datacols=cleanval[1] + cleanval[2]
	return (cleanval[0],(datacols))
	
def mergedop(x,y):
    val1=x
    val2=y
    val=val1+val2
    return (val)
	
tablescolumns=sc.textFile('/tmp/r660737/dedup_input_files/combined_file.txt',1)

tablescolumnsclean=tablescolumns.map(cleancolumns)
tablescolumnscleangrouped=tablescolumnsclean.reduceByKey(mergedop)
flipped=tablescolumnscleangrouped.map(lambda x: (x[1],x[0]))
grouped=flipped.groupByKey()
other_databases={}
key = 1
for i in grouped.collect():
    if (len(list(i[1])) > 1):
        other_databases[key] = list(i[1])
    key = key +1	
	
data = sc.parallelize([(k,)+(v,) for k,v in other_databases.items()]).toDF(['key','val'])
FinalDF = data.withColumn('TBL_ID', explode('val'))
key_df = FinalDF.select('key','TBL_ID')

*************************
--DATABASE details:

df_details = [StructField('DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Table_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details)
df2=spark.read.csv('/tmp/r660737/main_input_files/database_information_details2.txt', schema = final_details)

final_df = df2.join(key_df,trim(df2.Table_Id) == trim(key_df.TBL_ID)).select(df2.DB_name,key_df.TBL_ID,df2.Table_name,df2.Table_Type,key_df.key,df2.COLUMN_COUNT)

newDf = final_df.withColumn("DB_name",regexp_replace("DB_name", "'", "")).withColumn("Table_name",regexp_replace("Table_name", "'", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("TBL_ID",regexp_replace("TBL_ID", "L", ""))

****************************
--gold_copy

df_details1 = [StructField('Gold_DB_NAME',StringType(),True), StructField('Gold_TABLE_NAME',StringType(),True)]
final_details = StructType(fields=df_details1)
golden_copy_df=spark.read.csv('/tmp/i725369/Golden_copy_list.csv', schema = final_details)

newDf.registerTempTable('sc1')
golden_copy_df.registerTempTable('gold_copy')

resultset = spark.sql('select a.DB_name,a.TBL_ID,a.Table_name,a.Table_Type,trim(a.key)as DEDUP_KEY,a.COLUMN_COUNT,1 as flag,b.Gold_TABLE_NAME from sc1 a left join gold_copy b on (trim(a.DB_name)= trim(b.Gold_DB_NAME)) and (trim(a.Table_name)= trim(b.Gold_TABLE_NAME))')

resultset.registerTempTable('add_flag')

final=spark.sql('select DB_name,TBL_ID,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,case when Gold_TABLE_NAME is null then (flag*0)  else  flag end as golden_copy_key from add_flag')

*************************
--db_check_flag

final.registerTempTable('sc1_df')
test_df=spark.sql('select DEDUP_KEY,DB_name,count(*) as grouped_count from sc1_df group by DEDUP_KEY,DB_name')
test_df.registerTempTable('test_df')

t1_df=spark.sql('select a.DB_name,trim(a.TBL_ID)as Table_Id,a.Table_name,a.Table_Type,a.DEDUP_KEY,a.COLUMN_COUNT,a.golden_copy_key,b.grouped_count from sc1_df a left join test_df b on trim(a.DEDUP_KEY)=trim(b.DEDUP_KEY) and trim(a.DB_name)= trim(b.DB_name)')

t1_df.registerTempTable('t1_df')
t2_df=spark.sql('select DEDUP_KEY,count(*) as c1 from t1_df group by DEDUP_KEY')
t2_df.registerTempTable('t2_df')

t3_df=spark.sql('select a.DB_name,a.Table_Id,a.Table_name,a.Table_Type,a.DEDUP_KEY,a.COLUMN_COUNT,a.golden_copy_key,a.grouped_count,b.c1,1 as DB_check_flg  from t1_df a left join t2_df b on trim(a.DEDUP_KEY)=trim(b.DEDUP_KEY)')

t3_df.registerTempTable('t3_df')

t4_df=spark.sql('select DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,golden_copy_key,grouped_count,c1,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

t4_df.registerTempTable('t4_df')

Final_dbflag_df=spark.sql('select trim(DB_name) as DB_name ,trim(Table_Id) as Table_Id,trim(Table_name) as Table_name,trim(Table_Type) as Table_Type,trim(DEDUP_KEY) as DEDUP_KEY ,trim(COLUMN_COUNT) as COLUMN_COUNT,trim(golden_copy_key) as golden_copy_key ,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')


*********************************
--add create_time,access_count columns


df_hive = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('CREATE_TIME',StringType(),True)]
hive_details = StructType(fields=df_hive)
hive_df=spark.read.csv('/tmp/r660737/hive_m3.txt', schema = hive_details)


df_dum = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('Count',StringType(),True)]
dum_details = StructType(fields=df_dum)
dum_dfdf=spark.read.csv('/tmp/r660737/dum_mt.txt', schema = dum_details)

innerjoin_DF = dum_dfdf.join(hive_df, trim(dum_dfdf.DB_TBL) == trim( hive_df.DB_TBL)).select(dum_dfdf.DB_NAME,hive_df.DB_TBL,dum_dfdf.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

Left_join_DF = hive_df.join(dum_dfdf, trim(hive_df.DB_TBL) == trim(dum_dfdf.DB_TBL),how ='left').select(hive_df.DB_NAME,hive_df.DB_TBL,hive_df.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

remove_null_DF = Left_join_DF.where("Count is null").select(Left_join_DF.DB_NAME,Left_join_DF.DB_TBL,Left_join_DF.TBL_NAME,Left_join_DF.Count,Left_join_DF.CREATE_TIME)

finalDF = innerjoin_DF.union(remove_null_DF)

Final_dbflag_df.registerTempTable('a')
finalDF.registerTempTable('finalDF')

final_access_count_df=spark.sql('select trim(DB_NAME)as DB_NAME,trim(DB_TBL)as DB_TBL,trim(TBL_NAME)as TBL_NAME,trim(Count)as Count,trim(CREATE_TIME)as CREATE_TIME from finalDF')

sc1_db_check_flg_df=spark.sql('select DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,trim(a.golden_copy_key)as golden_copy_flag,DB_check_flg,concat(a.DB_name,"@",a.Table_name) as DB_TBL from a')

sc1_db_check_flg_df.registerTempTable('sc1')
final_access_count_df.registerTempTable('access_count')

sc1_final_inner_join=spark.sql('select sc1.DB_name,sc1.Table_Id,sc1.Table_name,sc1.Table_Type,sc1.DEDUP_KEY,sc1.COLUMN_COUNT,sc1.golden_copy_flag,sc1.DB_check_flg,access_count.Count,access_count.CREATE_TIME from sc1 join access_count on (sc1.DB_TBL == access_count.DB_TBL)')

time_conversion= sc1_final_inner_join.withColumn("CREATE_TIME",sc1_final_inner_join["CREATE_TIME"].cast(IntegerType()))
sc1_final_report_time=time_conversion.withColumn('CREATE_TIME',f.date_format(time_conversion.CREATE_TIME.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))

fill_na_df = sc1_final_report_time.fillna({'Count':'0'})
acces_time_df = fill_na_df.withColumn("Count", fill_na_df["Count"].cast(IntegerType()))

acces_time_df.write.csv("/tmp/i725369/db_flg_gold_reports/sc1_mt_discovery_db_flg01combined_for_tableau")

*******************************************END**********************************************
