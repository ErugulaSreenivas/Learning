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

mt_s_2
*************************************************

Combined Data input query:
select a.OBJECT_ID,a.COLUMN_NAME,a.DATA_TYPE  from (select  DBS.NAME AS OWNER,DBS.DB_ID,TBLS.TBL_NAME as OBJECT_NAME,TBLS.TBL_ID as OBJECT_ID,TBLS.TBL_TYPE as OBJECT_TYPE,COLUMNS_V2.COLUMN_NAME, COLUMNS_V2.COMMENT as COLUMN_DESCRIPTION, COLUMNS_V2.TYPE_NAME AS DATA_TYPE from DBS JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID JOIN SDS ON TBLS.SD_ID = SDS.SD_ID JOIN COLUMNS_V2 ON COLUMNS_V2.CD_ID = SDS.CD_ID and TBLS.TBL_TYPE NOT LIKE 'VIRTUAL_VIEW' and DBS.NAME in ('db_opsdata_raw','db_opsdata_refined','db_opsdata_curated','db_opsdata_raw_u','db_opsdata_refined_u','db_opsdata_utd','db_adi_services','db_adi_services_uat') )a order by a.OBJECT_ID,a.COLUMN_NAME,a.DATA_TYPE

Hive:
select a.NAME as DB_NAME,b.TBL_NAME,concat(a.NAME,'@',b.TBL_NAME) as DB_TBL,b.CREATE_TIME from DBS a join TBLS b on a.DB_ID = b.DB_ID

Dum_query:
select a.database_name as DB_NAME,a.tablename as TBL_NAME,concat(a.database_name,'@',a.tablename) as DB_TBL, count(a.ACTION) as Total_Count  from db_bdcs.v_dum360_ccb a where tablename not in('N/A', '') and platform ='Cloudera' and cluster ='MT-DISCOVERY'  and access_allowed_flag = 'Y' group by a.database_name,a.tablename order by Total_Count desc

Database details:
select a.OWNER, a.OBJECT_ID, a.OBJECT_NAME, a.OBJECT_TYPE, count(a.COLUMN_NAME) as COLUMN_COUNT from (select  DBS.NAME AS OWNER,DBS.DB_ID,TBLS.TBL_NAME as OBJECT_NAME,TBLS.TBL_ID as OBJECT_ID,TBLS.TBL_TYPE as OBJECT_TYPE,COLUMNS_V2.COLUMN_NAME, COLUMNS_V2.COMMENT as COLUMN_DESCRIPTION, COLUMNS_V2.TYPE_NAME AS DATA_TYPE from DBS JOIN TBLS ON DBS.DB_ID = TBLS.DB_ID JOIN SDS ON TBLS.SD_ID = SDS.SD_ID JOIN COLUMNS_V2 ON COLUMNS_V2.CD_ID = SDS.CD_ID and TBLS.TBL_TYPE NOT LIKE 'VIRTUAL_VIEW' and DBS.NAME in ('db_opsdata_raw','db_opsdata_refined','db_opsdata_curated','db_opsdata_raw_u','db_opsdata_refined_u','db_opsdata_utd','db_adi_services','db_adi_services_uat'))a group by a.OWNER, a.OBJECT_ID, a.OBJECT_NAME, a.OBJECT_TYPE order by COLUMN_COUNT desc

*********************
df_details = [StructField('DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Table_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('DEDUP_KEY',StringType(),True),StructField('COLUMN_COUNT',StringType(),True),StructField('golden_copy_key',StringType(),True)]
final_details = StructType(fields=df_details)
sc1_df=spark.read.csv('/tmp/i725369/Golden_copy_final_reports/Combined_report_scenario_1/', schema = final_details)

***********************
sc1_df.registerTempTable('sc1_df')
test_df=spark.sql('select DEDUP_KEY,DB_name,count(*) as grouped_count from sc1_df group by DEDUP_KEY,DB_name')
test_df.registerTempTable('test_df')

t1_df=spark.sql('select a.DB_name,a.Table_Id,a.Table_name,a.Table_Type,a.DEDUP_KEY,a.COLUMN_COUNT,a.golden_copy_key,b.grouped_count from sc1_df a left join test_df b on trim(a.DEDUP_KEY)=trim(b.DEDUP_KEY) and trim(a.DB_name)= trim(b.DB_name)')

t1_df.registerTempTable('t1_df')
t2_df=spark.sql('select DEDUP_KEY,count(*) as c1 from t1_df group by DEDUP_KEY')
t2_df.registerTempTable('t2_df')

t3_df=spark.sql('select a.DB_name,a.Table_Id,a.Table_name,a.Table_Type,a.DEDUP_KEY,a.COLUMN_COUNT,a.golden_copy_key,a.grouped_count,b.c1,1 as DB_check_flg  from t1_df a left join t2_df b on trim(a.DEDUP_KEY)=trim(b.DEDUP_KEY)')

t3_df.registerTempTable('t3_df')

t4_df=spark.sql('select DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,golden_copy_key,grouped_count,c1,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

t4_df.registerTempTable('t4_df')

Final_dbflag_df=spark.sql('select trim(DB_name) as DB_name ,trim(Table_Id) as Table_Id,trim(Table_name) as Table_name,trim(Table_Type) as Table_Type,trim(DEDUP_KEY) as DEDUP_KEY ,trim(COLUMN_COUNT) as COLUMN_COUNT,trim(golden_copy_key) as golden_copy_key ,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

Final_dbflag_df.registerTempTable('Final_dbflag_df')

Final_dbflag_df_0=spark.sql('select * from Final_dbflag_df where DB_check_flg = 0')

Final_dbflag_df_1=spark.sql('select * from Final_dbflag_df where DB_check_flg = 1')

********************************************
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
import pyspark
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t 
************************
--select a.NAME as DB_NAME,b.TBL_NAME,concat(a.NAME,'@',b.TBL_NAME) as DB_TBL,b.CREATE_TIME from DBS a join TBLS b on a.DB_ID = b.DB_ID

--Dum_query:

select a.database_name as DB_NAME,a.tablename as TBL_NAME,concat(a.database_name,'@',a.tablename) as DB_TBL, count(a.ACTION) as Total_Count  from db_bdcs.v_dum360_ccb a where tablename not in('N/A', '') and platform ='Cloudera' and cluster ='MT-DISCOVERY'  and access_allowed_flag = 'Y' group by a.database_name,a.tablename order by Total_Count desc

/tmp/r660737/hive_m3.txt

/tmp/r660737/dum_mt.txt
********************************
from pyspark.sql import functions as f

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

************************

Final_dbflag_df_0.registerTempTable('sc1_df')

sc1_final_df = spark.sql("select trim(DB_name) as DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,trim(golden_copy_key)as golden_copy_flag,DB_check_flg,concat(DB_name,'@',Table_name) as DB_TBL from sc1_df")

finalDF.registerTempTable('finalDF')
combined_df = spark.sql("select DB_TBL,Count,trim(CREATE_TIME) as CREATE_TIME from finalDF")

Merge_DF = sc1_final_df.join(combined_df, trim(sc1_final_df.DB_TBL) == trim( combined_df.DB_TBL)).select(sc1_final_df.DB_name,sc1_final_df.Table_Id,sc1_final_df.Table_name,sc1_final_df.DEDUP_KEY,sc1_final_df.COLUMN_COUNT,sc1_final_df.golden_copy_flag,sc1_final_df.DB_check_flg,combined_df.Count,combined_df.CREATE_TIME)

returnDF = Merge_DF.fillna({'Count':'0'})

data_df = returnDF.withColumn("Count", returnDF["Count"].cast(IntegerType()))

time_cnv_df = data_df.withColumn("CREATE_TIME", data_df["CREATE_TIME"].cast(IntegerType()))
Hive_new_df=time_cnv_df.withColumn('CREATE_TIME', f.date_format(time_cnv_df.CREATE_TIME.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))

Hive_new_df.registerTempTable('Hive_new_df')

combined_df = spark.sql("select * from Hive_new_df where DB_name in ('db_opsdata_raw','db_opsdata_refined','db_opsdata_curated','db_opsdata_raw_u','db_opsdata_refined_u','db_opsdata_utd','db_adi_services','db_adi_services_uat')")

****
from pyspark.sql.functions import desc

Sorted_df=combined_df.sort(desc("DEDUP_KEY"),desc("Count"))

Sorted_df.registerTempTable('Sorted_df')
gold_df=spark.sql('select distinct trim(DEDUP_KEY) as DEDUP_KEY  from Sorted_df where golden_copy_flag = 1')

combined_df.registerTempTable('combined_df')
dedup_df=spark.sql('select distinct trim(DEDUP_KEY) as DEDUP_KEY from combined_df')

substract_gold_df=dedup_df.subtract(gold_df)

substract_gold_df.registerTempTable('substract_gold_df')

subtract_gold_query_df=spark.sql('select * from  Sorted_df where DEDUP_KEY in(select substract_gold_df.DEDUP_KEY from substract_gold_df)')

final_result_df=subtract_gold_query_df.dropDuplicates(subset = ['DEDUP_KEY'])

gold_result=spark.sql('select * from Sorted_df where golden_copy_flag = 1')

union_df_result=final_result_df.union(gold_result)

************
union_df_result.registerTempTable('union_df_result')
Final_dbflag_df_1.registerTempTable('diffent_db')

union_df=spark.sql('select distinct(Table_Id) from union_df_result')
df_diffent_db =spark.sql('select distinct Table_Id from diffent_db') 

final_input_mt_discovery_sc2=df_diffent_db.union(union_df)

******************************

def cleancolumns(x):
    firstbrace=x.find('(')
    lastbrace=x.rfind(')')
    cleaneddata=x[firstbrace+1:lastbrace]
    cleanval=cleaneddata.split(',',2)
    column1=cleanval[1]
    datatype=cleanval[2]
    return (cleanval[0],column1,datatype)

combineddf=sc.textFile('/tmp/r660737/dedup_input_files/ops_data_input_tblcoldtyp.txt',4)


combined_map=combineddf.map(cleancolumns)

comb_final_df=spark.createDataFrame(combined_map,('tableid','columnname','datatype'))

combined_master_Df = comb_final_df.withColumn("tableid",regexp_replace("tableid", "L", "")).withColumn("columnname",regexp_replace("columnname", "'", "")).withColumn("datatype",regexp_replace("datatype", "'", ""))


combined_master_Df.registerTempTable('combined_master_Df')

master_inputdf=spark.sql('select trim(tableid)as tableid,trim(columnname)as columnname,trim(datatype)as datatype from combined_master_Df')

final_input_mt_discovery_sc2.registerTempTable('final_input_mt_discovery_sc2')
master_inputdf.registerTempTable('master_inputdf')

mt_discover_input_df=spark.sql('select master_inputdf.tableid,master_inputdf.columnname,master_inputdf.datatype from final_input_mt_discovery_sc2 join master_inputdf on final_input_mt_discovery_sc2.Table_Id== master_inputdf.tableid')

mt_discover_input_df.registerTempTable('mt_discover_input_df')

*********************************

combinedf=spark.sql('select a.tableid as tableid1,b.tableid as tableid2,1 as cnt from mt_discover_input_df a join mt_discover_input_df b where a.columnname=b.columnname and a.datatype=b.datatype')

combinedf.registerTempTable('combinedf')
combinedfaggregate=spark.sql('select tableid1,tableid2,count(cnt) as cnt from combinedf group by tableid1,tableid2').cache()
combinedfaggregate.registerTempTable('combinedfaggregate')
combinedfgrp=spark.sql('select tableid,count(*) as columncount from mt_discover_input_df group by tableid').cache()
combinedfgrp.show(1)
combinedfgrp.registerTempTable('combinedfgrp')

resultset=spark.sql('select  a.tableid1,a.tableid2,round(cnt/columncount*100) as match from combinedfaggregate a join combinedfgrp b on a.tableid1=b.tableid')
resultsetfiltered=resultset.filter((resultset.match > 80)& (resultset.match<100))

resultsetfiltered.registerTempTable('resultsetfiltered')

filteredresultsetfiltereddupstbl1=spark.sql('select a.tableid1,a.tableid2,a.match,b.columncount as tableid1cnt from resultsetfiltered a LEFT join combinedfgrp b  on a.tableid1=b.tableid ')
filteredresultsetfiltereddupstbl2=spark.sql('select a.tableid1,a.tableid2,a.match,b.columncount as tableid2cnt from resultsetfiltered a LEFT join combinedfgrp b  on a.tableid2=b.tableid')
filteredresultsetfiltereddupstbl1.registerTempTable('table1cnt')
filteredresultsetfiltereddupstbl2.registerTempTable('table2cnt')


finalop=spark.sql('select a.tableid1,a.tableid2,a.match,a.tableid1cnt,b.tableid2cnt from table1cnt a left join table2cnt b on a.tableid1=b.tableid1 and a.tableid2=b.tableid2 and a.match=b.match' )
finalop.registerTempTable('finalop')

finalopreport=spark.sql('select tableid1,tableid2,match,tableid1cnt,tableid2cnt,case when (tableid1cnt > tableid2cnt) then round(tableid2cnt/tableid1cnt*100) else  round(tableid1cnt/tableid2cnt*100 )end as columcntpercentage from finalop')

finalopreport.registerTempTable('test')
df=spark.sql('select tableid1,tableid2,tableid1cnt,tableid2cnt,match,columcntpercentage,1 as flag  from test')
df.registerTempTable('df')
df1=spark.sql('select tableid1,tableid2,match,tableid1cnt,tableid2cnt,columcntpercentage,case when (columcntpercentage < 50 ) then flag  else (flag*0)end as flag_key from df')

df_details = [StructField('Champion_DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Champion_Tbl_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details)
db_details=spark.read.csv('/tmp/r660737/dedup_input_files/ops_data_db_details.txt',schema = final_details)

df_detail_tmp = db_details.withColumn("Champion_DB_name",regexp_replace("Champion_DB_name", "'", "")).withColumn("Champion_Tbl_name",regexp_replace("Champion_Tbl_name", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("Table_Id",regexp_replace("Table_Id", "L", ""))

df_detail = df_detail_tmp.withColumn("Champion_DB_name",regexp_replace("Champion_DB_name", "\(", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "\)", ""))

parent_df = df1.join(df_detail,trim(df1.tableid1) == trim(df_detail.Table_Id)).select(df_detail.Champion_DB_name,df1.tableid1,df_detail.Champion_Tbl_name,df1.tableid2,df1.match,df1.columcntpercentage,df1.flag_key,df1.tableid1cnt,df1.tableid2cnt)

df_details_1 = [StructField('Challenger_DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Challenger_Tbl_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details_1)
db_details1=spark.read.csv('/tmp/r660737/dedup_input_files/ops_data_db_details.txt', schema = final_details)

df_detail_childtmp= db_details1.withColumn("Challenger_DB_name",regexp_replace("Challenger_DB_name", "'", "")).withColumn("Challenger_Tbl_name",regexp_replace("Challenger_Tbl_name", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("Table_Id",regexp_replace("Table_Id", "L", ""))

df_detail1 = df_detail_childtmp.withColumn("Challenger_DB_name",regexp_replace("Challenger_DB_name", "\(", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "\)", ""))

children_df = df1.join(df_detail1,trim(df1.tableid2) == trim(df_detail1.Table_Id)).select(df1.tableid1,df1.tableid2,df_detail1.Challenger_Tbl_name,df_detail1.Challenger_DB_name,df1.match,df1.columcntpercentage,df1.flag_key,df1.tableid1cnt,df1.tableid2cnt)


parent_df.registerTempTable("Parent")
children_df.registerTempTable("Child")

df_result  = sqlContext.sql("select a.Champion_DB_name,a.Champion_Tbl_name,trim(a.tableid1cnt)as Chmp_Tbl_Column_Cnt,b.Challenger_DB_name,b.Challenger_Tbl_name,trim(b.tableid2cnt)as Chlg_Tbl_Column_Cnt ,b.match,b.flag_key from Parent a join child b on a.tableid1= b.tableid1 and a.tableid2=b.tableid2")

df_result.write.csv("/tmp/i725369/db_flg_gold_reports/mt_dicovery_subset_ops_data_tmp0809")


*****************************

df_details = [StructField('Champion_DB_name',StringType(),True), StructField('Champion_Tbl_name',StringType(),True),StructField('Chmp_Tbl_Column_Cnt',StringType(),True),StructField('Challenger_DB_name',StringType(),True),StructField('Challenger_Tbl_name',StringType(),True),StructField('Chlg_Tbl_Column_Cnt',StringType(),True),StructField('match',StringType(),True),StructField('flag_key',StringType(),True)]
final_details = StructType(fields=df_details)
df_detail112=spark.read.csv('/tmp/i725369/db_flg_gold_reports/mt_dicovery_subset_ops_data_tmp0809/*.csv', schema = final_details)


df_details1 = [StructField('Gold_DB_NAME',StringType(),True), StructField('Gold_TABLE_NAME',StringType(),True)]
final_details = StructType(fields=df_details1)
golden_copy_df=spark.read.csv('/tmp/i725369/Golden_copy_list.csv', schema = final_details)

df_detail112.registerTempTable('sc1')
golden_copy_df.registerTempTable('gold_copy')

resultset = spark.sql('select a.Champion_DB_name,a.Champion_Tbl_name,a.Chmp_Tbl_Column_Cnt,a.Challenger_DB_name,a.Challenger_Tbl_name,a.Chlg_Tbl_Column_Cnt,a.match,a.flag_key,1 as flag,b.Gold_TABLE_NAME from sc1 a left join gold_copy b on (trim(a.Champion_DB_name)= trim(b.Gold_DB_NAME)) and (trim(a.Champion_Tbl_name)= trim(b.Gold_TABLE_NAME))')

resultset.registerTempTable('add_flag')

final=spark.sql('select Champion_DB_name,Champion_Tbl_name,Chmp_Tbl_Column_Cnt,Challenger_DB_name,Challenger_Tbl_name,Chlg_Tbl_Column_Cnt,match,flag_key,case when Gold_TABLE_NAME is null then (flag*0)  else  flag end as Champion_gold_key from add_flag')

final.registerTempTable('challenger_flag')

resultset_1 = spark.sql('select a.Champion_DB_name,a.Champion_Tbl_name,a.Chmp_Tbl_Column_Cnt,a.Champion_gold_key,a.Challenger_DB_name,a.Challenger_Tbl_name,a.Chlg_Tbl_Column_Cnt,a.match,a.flag_key,1 as flag,b.Gold_TABLE_NAME from challenger_flag a left join gold_copy b on (trim(a.Challenger_DB_name)= trim(b.Gold_DB_NAME)) and (trim(a.Challenger_Tbl_name)= trim(b.Gold_TABLE_NAME))')

resultset_1.registerTempTable('add_flag_1')

final_1=spark.sql('select Champion_DB_name,Champion_Tbl_name,Chmp_Tbl_Column_Cnt,Champion_gold_key,Challenger_DB_name,Challenger_Tbl_name,Chlg_Tbl_Column_Cnt,case when Gold_TABLE_NAME is null then (flag*0)  else  flag end as Challenger_gold_key,match,flag_key from add_flag_1')

final_1.write.csv("/tmp/i725369/db_flg_gold_reports/mt_dicovery_subset_ops_data_aug_09")
***************************************END*****************************************


********************************************************************
mp_c_sc-1
*****************************************************************
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
                

tablescolumns=sc.textFile('/tmp/i725369/map_cea_input_files/combined_data_exclue_sid.txt',1)


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
                
result = sc.parallelize(other_databases)
data = sc.parallelize([(k,)+(v,) for k,v in other_databases.items()]).toDF(['key','val'])
FinalDF = data.withColumn('TBL_ID', explode('val'))
NewDF = FinalDF.select('key','TBL_ID')

df_details = [StructField('DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Table_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details)
df2=spark.read.csv('/tmp/i725369/map_cea_input_files/db_information_details.txt', schema = final_details)



final_df = df2.join(NewDF,trim(df2.Table_Id) == trim(NewDF.TBL_ID)).select(df2.DB_name,NewDF.TBL_ID,df2.Table_name,df2.Table_Type,NewDF.key,df2.COLUMN_COUNT)

step_1_df = final_df.withColumn("DB_name",regexp_replace("DB_name", "'", "")).withColumn("Table_name",regexp_replace("Table_name", "'", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("TBL_ID",regexp_replace("TBL_ID", "L", ""))

******************************************************************

step_1_df.registerTempTable('step_1_df')
test_df=spark.sql('select key,DB_name,count(*) as grouped_count from step_1_df group by key,DB_name')
test_df.registerTempTable('test_df')

t1_df=spark.sql('select a.DB_name,a.TBL_ID,a.Table_name,a.Table_Type,a.key,a.COLUMN_COUNT,b.grouped_count from step_1_df a left join test_df b on trim(a.key)=trim(b.key) and trim(a.DB_name)= trim(b.DB_name)')

t1_df.registerTempTable('t1_df')
t2_df=spark.sql('select key,count(*) as c1 from t1_df group by key')
t2_df.registerTempTable('t2_df')

t3_df=spark.sql('select a.DB_name,a.TBL_ID,a.Table_name,a.Table_Type,a.key,a.COLUMN_COUNT,a.grouped_count,b.c1,1 as DB_check_flg  from t1_df a left join t2_df b on trim(a.key)=trim(b.key)')

t3_df.registerTempTable('t3_df')

t4_df=spark.sql('select DB_name,TBL_ID,Table_name,Table_Type,key,COLUMN_COUNT,grouped_count,c1,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

t4_df.registerTempTable('t4_df')

Final_dbflag_df=spark.sql('select trim(DB_name) as DB_name ,trim(TBL_ID) as TBL_ID,trim(Table_name) as Table_name,trim(Table_Type) as Table_Type,trim(key) as key ,trim(COLUMN_COUNT) as COLUMN_COUNT,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

Final_dbflag_df.write.csv("/tmp/i725369/map_cea_output_files/sc1_map_cea_output_07_31")

*****************************************************END MAP_CEA_SC1*****************************************************

df_hive = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('CREATE_TIME',StringType(),True)]
hive_details = StructType(fields=df_hive)
hive_df=spark.read.option("sep", "\t").csv("/tmp/i725369/map_cea_input_files/subset_files/hive_metastore_mapcea_dbtbl.csv", schema = hive_details)


df_dum = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('Count',StringType(),True)]
dum_details = StructType(fields=df_dum)
dum_dfdf=spark.read.csv("/tmp/i725369/map_cea_input_files/subset_files/Map_Cea_Dum.csv", schema = dum_details)


innerjoin_DF = dum_dfdf.join(hive_df, trim(dum_dfdf.DB_TBL) == trim( hive_df.DB_TBL)).select(dum_dfdf.DB_NAME,hive_df.DB_TBL,dum_dfdf.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

Left_join_DF = hive_df.join(dum_dfdf, trim(hive_df.DB_TBL) == trim(dum_dfdf.DB_TBL),how ='left').select(hive_df.DB_NAME,hive_df.DB_TBL,hive_df.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

remove_null_DF = Left_join_DF.where("Count is null").select(Left_join_DF.DB_NAME,Left_join_DF.DB_TBL,Left_join_DF.TBL_NAME,Left_join_DF.Count,Left_join_DF.CREATE_TIME)

returnDF = remove_null_DF.fillna({'Count':'0'})

finalDF = innerjoin_DF.union(remove_null_DF)

********************************

df_details = [StructField('DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Table_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('DEDUP_KEY',StringType(),True),StructField('COLUMN_COUNT',StringType(),True),StructField('DB_check_flg',StringType(),True)]
final_details = StructType(fields=df_details)
sc1_df=spark.read.csv('/tmp/i725369/map_cea_output_files/sc1_map_cea_output_07_31/*.csv', schema = final_details)


finalDF.registerTempTable('finalDF')
sc1_df.registerTempTable('a')

sc1_final_df = spark.sql("select trim(DB_name) as DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,DB_check_flg,concat(DB_name,'@',Table_name) as DB_TBL from a")

final_access_count_df=spark.sql('select trim(DB_NAME)as DB_NAME,trim(DB_TBL)as DB_TBL,trim(TBL_NAME)as TBL_NAME,trim(Count)as Count,trim(CREATE_TIME)as CREATE_TIME from finalDF')

sc1_db_check_flg_df.registerTempTable('sc1')
final_access_count_df.registerTempTable('access_count')

sc1_final_inner_join=spark.sql('select sc1.DB_name,sc1.Table_Id,sc1.Table_name,sc1.Table_Type,sc1.DEDUP_KEY,sc1.COLUMN_COUNT,sc1.DB_check_flg,access_count.Count,access_count.CREATE_TIME from sc1 join access_count on (sc1.DB_TBL == access_count.DB_TBL)')

returnDF1 = sc1_final_inner_join.fillna({'Count':'0'})

from pyspark.sql import functions as f
from pyspark.sql import types as t 


time_conversion= returnDF1.withColumn("CREATE_TIME",returnDF1["CREATE_TIME"].cast(IntegerType()))
sc1_final_report_time_map_cea=time_conversion.withColumn('CREATE_TIME',f.date_format(time_conversion.CREATE_TIME.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))


sc1_final_report_time_map_cea.write.csv("/tmp/i725369/map_cea_output_files/sc1_map_cea_output_08_01")


end*************
**********************************************************


Final_dbflag_df.registerTempTable('Final_dbflag_df')

Final_dbflag_df_0=spark.sql('select * from Final_dbflag_df where DB_check_flg = 0')

Final_dbflag_df_1=spark.sql('select * from Final_dbflag_df where DB_check_flg = 1')

Final_dbflag_df_1.registerTempTable('Final_dbflag_df_1')

Final_dbflag_df_different_dbs=spark.sql('select DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,golden_copy_key,case when Final_dbflag_df_1.DB_name =="db_cibda_gib_hconf_derived" then (DB_check_flg) when Final_dbflag_df_1.DB_name =="db_opsdata_curated" then (DB_check_flg *2) when Final_dbflag_df_1.DB_name =="db_rqd_crdt_results_pii" then (DB_check_flg *3) when Final_dbflag_df_1.DB_name =="db_opsdata_con_ops" then (DB_check_flg *4) when Final_dbflag_df_1.DB_name =="db_ccbml_clnrm7_views" then (DB_check_flg *5) when Final_dbflag_df_1.DB_name =="db_dgo_wpd_refined" then (DB_check_flg *6) when Final_dbflag_df_1.DB_name =="db_amp_jpm_poc_landing" then (DB_check_flg *7) when Final_dbflag_df_1.DB_name =="db_crs_dataops" then (DB_check_flg *8) when Final_dbflag_df_1.DB_name =="db_rqd_ckfd_results_pii" then (DB_check_flg *9) when Final_dbflag_df_1.DB_name =="db_cce_private" then (DB_check_flg *10) when Final_dbflag_df_1.DB_name =="db_aml_mrg_hivedb1" then (DB_check_flg *11) when Final_dbflag_df_1.DB_name =="db_rqd_ceclval_source_pii" then (DB_check_flg *12) when Final_dbflag_df_1.DB_name =="db_gwm_cti" then (DB_check_flg *13) when Final_dbflag_df_1.DB_name =="db_rqd_inteldev_results_pii" then (DB_check_flg *14) when Final_dbflag_df_1.DB_name =="db_gwm_ids_realestate_sandbox" then (DB_check_flg *15) when Final_dbflag_df_1.DB_name =="db_rqd_awm_source" then (DB_check_flg *16) when Final_dbflag_df_1.DB_name =="dataops" then (DB_check_flg *17) when Final_dbflag_df_1.DB_name =="db_opsdata_con_af" then (DB_check_flg *18) when Final_dbflag_df_1.DB_name =="db_crs_di_pii" then (DB_check_flg *19) when Final_dbflag_df_1.DB_name =="db_ccb_dsdisc_ws3" then (DB_check_flg *20) when Final_dbflag_df_1.DB_name =="db_crs_analysis_private_t" then (DB_check_flg *21) when Final_dbflag_df_1.DB_name =="db_opsdata_con_mb" then (DB_check_flg *22) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_curated" then (DB_check_flg *23) when Final_dbflag_df_1.DB_name =="institute_db_retail" then (DB_check_flg *24) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_canada" then (DB_check_flg *25) when Final_dbflag_df_1.DB_name =="db_crs_de_pii" then (DB_check_flg *26) when Final_dbflag_df_1.DB_name =="db_opsdata_con_ccm" then (DB_check_flg *27) when Final_dbflag_df_1.DB_name =="institute_retail_sandbox" then (DB_check_flg *28) when Final_dbflag_df_1.DB_name =="db_bdcs" then (DB_check_flg *29) when Final_dbflag_df_1.DB_name =="db_rqd_awm_sandbox" then (DB_check_flg *30) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_finance" then (DB_check_flg *31) when Final_dbflag_df_1.DB_name =="db_rqd_inv_ds_sandbox_pii" then (DB_check_flg *32) when Final_dbflag_df_1.DB_name =="db_opsdata_con_cb" then (DB_check_flg *33) when Final_dbflag_df_1.DB_name =="db_aml_global_hivedb1" then (DB_check_flg *34) when Final_dbflag_df_1.DB_name =="db_rqd_inv_bb_sandbox_pii" then (DB_check_flg *35) when Final_dbflag_df_1.DB_name =="db_opsdata_con_cwm" then (DB_check_flg *36) when Final_dbflag_df_1.DB_name =="db_crs_analysis_pii_t" then (DB_check_flg *37) when Final_dbflag_df_1.DB_name =="db_audit_am" then (DB_check_flg *38) when Final_dbflag_df_1.DB_name =="tcp_ods" then (DB_check_flg *39) when Final_dbflag_df_1.DB_name =="db_aria_eqr_final" then (DB_check_flg *40) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_cb" then (DB_check_flg *41) when Final_dbflag_df_1.DB_name =="db_bdcs_bdam_stg" then (DB_check_flg *42) when Final_dbflag_df_1.DB_name =="db_intellistor" then (DB_check_flg *43) when Final_dbflag_df_1.DB_name =="db_opsdata_refined" then (DB_check_flg *44) when Final_dbflag_df_1.DB_name =="db_rqd_cecl_source_pii" then (DB_check_flg *45) when Final_dbflag_df_1.DB_name =="db_crs_services_t" then (DB_check_flg *46) when Final_dbflag_df_1.DB_name =="institute_raw" then (DB_check_flg *47) when Final_dbflag_df_1.DB_name =="db_opsdata_con_dgt" then (DB_check_flg *48) when Final_dbflag_df_1.DB_name =="db_ccbml_turing_wksd_pii" then (DB_check_flg *49) when Final_dbflag_df_1.DB_name =="db_rqd_crdt_results" then (DB_check_flg *50) when Final_dbflag_df_1.DB_name =="db_atscale_udf" then (DB_check_flg *51) when Final_dbflag_df_1.DB_name =="db_aria_qds_staging" then (DB_check_flg *52) when Final_dbflag_df_1.DB_name =="db_rqd_cecl_source" then (DB_check_flg *53) when Final_dbflag_df_1.DB_name =="sravdb" then (DB_check_flg *54) when Final_dbflag_df_1.DB_name =="db_rqd_mrgr" then (DB_check_flg *55) when Final_dbflag_df_1.DB_name =="db_ccbml_clnrm3" then (DB_check_flg *56) when Final_dbflag_df_1.DB_name =="db_rqd_ceclval_sandbox_pii" then (DB_check_flg *57) when Final_dbflag_df_1.DB_name =="institute_markets" then (DB_check_flg *58) when Final_dbflag_df_1.DB_name =="db_tenant_admin" then (DB_check_flg *59) when Final_dbflag_df_1.DB_name =="db_opsdata_raw_u" then (DB_check_flg *60) when Final_dbflag_df_1.DB_name =="db_crs_analysis_non_pii_t" then (DB_check_flg *61) when Final_dbflag_df_1.DB_name =="db_merlin_sandbox" then (DB_check_flg *62) when Final_dbflag_df_1.DB_name =="db_rqd_debt" then (DB_check_flg *63) when Final_dbflag_df_1.DB_name =="db_wss_time" then (DB_check_flg *64) when Final_dbflag_df_1.DB_name =="db_rqd_crdt" then (DB_check_flg *65) when Final_dbflag_df_1.DB_name =="db_daas_result_store" then (DB_check_flg *66) when Final_dbflag_df_1.DB_name =="db_dgo_wpd_curated" then (DB_check_flg *67) when Final_dbflag_df_1.DB_name =="institute_business" then (DB_check_flg *68) when Final_dbflag_df_1.DB_name =="db_audit_ccb" then (DB_check_flg *69) when Final_dbflag_df_1.DB_name =="db_rqd_debt_pii" then (DB_check_flg *70) when Final_dbflag_df_1.DB_name =="db_cdsomtd_payments_derived" then (DB_check_flg *71) when Final_dbflag_df_1.DB_name =="db_dgo_wpd_raw" then (DB_check_flg *72) when Final_dbflag_df_1.DB_name =="db_aml_amlfraud_hivedb2" then (DB_check_flg *73) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_product" then (DB_check_flg *74) when Final_dbflag_df_1.DB_name =="db_rqd_inv_source" then (DB_check_flg *75) when Final_dbflag_df_1.DB_name =="db_mrchsvc_analytics" then (DB_check_flg *76) when Final_dbflag_df_1.DB_name =="db_ccbml_turing_wksb_pii" then (DB_check_flg *77) when Final_dbflag_df_1.DB_name =="db_rqd_sna_sandbox" then (DB_check_flg *78) when Final_dbflag_df_1.DB_name =="institute_lcc" then (DB_check_flg *79) when Final_dbflag_df_1.DB_name =="db_opsdata_raw" then (DB_check_flg *80) when Final_dbflag_df_1.DB_name =="db_aml_dataops_raw" then (DB_check_flg *81) when Final_dbflag_df_1.DB_name =="db_opsdata_con_ccs" then (DB_check_flg *82) when Final_dbflag_df_1.DB_name =="institute_markets_sandbox" then (DB_check_flg *83) when Final_dbflag_df_1.DB_name =="db_rqd_pem_sandbox" then (DB_check_flg *84) when Final_dbflag_df_1.DB_name =="db_rqd_ckfd" then (DB_check_flg *85) when Final_dbflag_df_1.DB_name =="db_opsdata_con_adi" then (DB_check_flg *86) when Final_dbflag_df_1.DB_name =="db_cdsomtd_cdso" then (DB_check_flg *87) when Final_dbflag_df_1.DB_name =="db_amp_jpm_pilot_landing" then (DB_check_flg *88) when Final_dbflag_df_1.DB_name =="institute_tde" then (DB_check_flg *89) when Final_dbflag_df_1.DB_name =="db_rqd_inteldev_sandbox" then (DB_check_flg *90) when Final_dbflag_df_1.DB_name =="db_aml_amlfraud_hivedb1" then (DB_check_flg *91) when Final_dbflag_df_1.DB_name =="db_apac_aml_fraud_curated" then (DB_check_flg *92) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_af" then (DB_check_flg *93) when Final_dbflag_df_1.DB_name =="db_crs_analysis_v" then (DB_check_flg *94) when Final_dbflag_df_1.DB_name =="db_rqd_cecl_sandbox" then (DB_check_flg *95) when Final_dbflag_df_1.DB_name =="db_rqd_research_sandbox" then (DB_check_flg *96) when Final_dbflag_df_1.DB_name =="db_ccbml_clnrm5_views" then (DB_check_flg *97) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_adi" then (DB_check_flg *98) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_cwm" then (DB_check_flg *99) when Final_dbflag_df_1.DB_name =="db_gwm_ids_rawdata" then (DB_check_flg *100) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_ccm" then (DB_check_flg *101) when Final_dbflag_df_1.DB_name =="db_mrchsvc_atscale_decs" then (DB_check_flg *102) when Final_dbflag_df_1.DB_name =="db_crs_services_uat_t" then (DB_check_flg *103) when Final_dbflag_df_1.DB_name =="db_ccbml_card_source" then (DB_check_flg *104) when Final_dbflag_df_1.DB_name =="db_audit_et" then (DB_check_flg *105) when Final_dbflag_df_1.DB_name =="tcp_stage" then (DB_check_flg *106) when Final_dbflag_df_1.DB_name =="db_ccbml_turing_wksa_pii" then (DB_check_flg *107) when Final_dbflag_df_1.DB_name =="db_crs_analysis_anon_v" then (DB_check_flg *108) when Final_dbflag_df_1.DB_name =="db_aria_qds_raw" then (DB_check_flg *109) when Final_dbflag_df_1.DB_name =="db_ccb_dsdisc_ws2" then (DB_check_flg *110) when Final_dbflag_df_1.DB_name =="db_crs_acxiom_anon_t" then (DB_check_flg *111) when Final_dbflag_df_1.DB_name =="db_mrchsvc_atscale_fnce" then (DB_check_flg *112) when Final_dbflag_df_1.DB_name =="db_opsdata_refined_u" then (DB_check_flg *113) when Final_dbflag_df_1.DB_name =="institute_db_thirdparty" then (DB_check_flg *114) when Final_dbflag_df_1.DB_name =="institute_db_wholesale" then (DB_check_flg *115) when Final_dbflag_df_1.DB_name =="db_crs_ext3_pvt_t" then (DB_check_flg *116) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_bb" then (DB_check_flg *117) when Final_dbflag_df_1.DB_name =="db_dzwsale_dataops" then (DB_check_flg *118) when Final_dbflag_df_1.DB_name =="db_gwm_ids_curated" then (DB_check_flg *119) when Final_dbflag_df_1.DB_name =="db_ccbml_opt_bcs_pii" then (DB_check_flg *120) when Final_dbflag_df_1.DB_name =="db_audit_ca" then (DB_check_flg *121) when Final_dbflag_df_1.DB_name =="db_audit_cib" then (DB_check_flg *122) when Final_dbflag_df_1.DB_name =="db_custdi_analytics_pii" then (DB_check_flg *123) when Final_dbflag_df_1.DB_name =="db_opsdata_refined_d" then (DB_check_flg *124) when Final_dbflag_df_1.DB_name =="db_rqd_debt_results" then (DB_check_flg *125) when Final_dbflag_df_1.DB_name =="db_apac_aml_amlfraud_hivedb3" then (DB_check_flg *126) when Final_dbflag_df_1.DB_name =="db_crs_ext2_pii_t" then (DB_check_flg *127) when Final_dbflag_df_1.DB_name =="db_rqd_debt_results_pii" then (DB_check_flg *128) when Final_dbflag_df_1.DB_name =="db_aml_amlfiu_hivedb1" then (DB_check_flg *129) when Final_dbflag_df_1.DB_name =="db_opsdata_utd" then (DB_check_flg *130) when Final_dbflag_df_1.DB_name =="db_rqd_ckfd_pii" then (DB_check_flg *131) when Final_dbflag_df_1.DB_name =="db_cdsomtdpub_cdso" then (DB_check_flg *132) when Final_dbflag_df_1.DB_name =="db_ccbml_opt_icr_pii" then (DB_check_flg *133) when Final_dbflag_df_1.DB_name =="db_dzistor_customer" then (DB_check_flg *134) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_ops" then (DB_check_flg *135) when Final_dbflag_df_1.DB_name =="db_opsdata_con_mkt" then (DB_check_flg *136) when Final_dbflag_df_1.DB_name =="db_jpmisbdo" then (DB_check_flg *137) when Final_dbflag_df_1.DB_name =="db_ccbml_opt_source" then (DB_check_flg *138) when Final_dbflag_df_1.DB_name =="db_adi_services_uat" then (DB_check_flg *139) when Final_dbflag_df_1.DB_name =="db_aria_eqr_staging" then (DB_check_flg *140) when Final_dbflag_df_1.DB_name =="db_rqd_inv_ds_sandbox" then (DB_check_flg *141) when Final_dbflag_df_1.DB_name =="db_rqd_inv_bb_sandbox" then (DB_check_flg *142) when Final_dbflag_df_1.DB_name =="db_rqd_ceclval_sandbox" then (DB_check_flg *143) when Final_dbflag_df_1.DB_name =="db_ioi" then (DB_check_flg *144) when Final_dbflag_df_1.DB_name =="db_crs_c360score_non_pii_t" then (DB_check_flg *145) when Final_dbflag_df_1.DB_name =="tcp_landing" then (DB_check_flg *146) when Final_dbflag_df_1.DB_name =="db_cibda_gib_all_derived" then (DB_check_flg *147) when Final_dbflag_df_1.DB_name =="db_cdsomtdpub_cdso_test" then (DB_check_flg *148) when Final_dbflag_df_1.DB_name =="db_rqd_crdt_pii" then (DB_check_flg *149) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_gcc" then (DB_check_flg *150) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_mkt" then (DB_check_flg *151) when Final_dbflag_df_1.DB_name =="db_cdsomtd_payments_base" then (DB_check_flg *152) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_refined" then (DB_check_flg *153) when Final_dbflag_df_1.DB_name =="db_crs_ext1_pii_t" then (DB_check_flg *154) when Final_dbflag_df_1.DB_name =="db_aml_fraud_refined" then (DB_check_flg *155) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_dgt" then (DB_check_flg *156) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_clv" then (DB_check_flg *157) when Final_dbflag_df_1.DB_name =="db_icr_sandbox_temp_pii" then (DB_check_flg *158) when Final_dbflag_df_1.DB_name =="db_rqd_inteldev_results" then (DB_check_flg *159) when Final_dbflag_df_1.DB_name =="db_opsdata_con_gcc" then (DB_check_flg *160) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_ccb" then (DB_check_flg *161) when Final_dbflag_df_1.DB_name =="db_dataops" then (DB_check_flg *162) when Final_dbflag_df_1.DB_name =="db_rqd_sna_results" then (DB_check_flg *163) when Final_dbflag_df_1.DB_name =="db_cibda_gib_conf_derived" then (DB_check_flg *164) when Final_dbflag_df_1.DB_name =="db_ccbml_turing_wksg" then (DB_check_flg *165) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_migrations" then (DB_check_flg *166) when Final_dbflag_df_1.DB_name =="db_wcr_wcml_hivepriv" then (DB_check_flg *167) when Final_dbflag_df_1.DB_name =="db_gwm_ids_mas_sandbox" then (DB_check_flg *168) when Final_dbflag_df_1.DB_name =="institute_cra" then (DB_check_flg *169) when Final_dbflag_df_1.DB_name =="db_test_aml" then (DB_check_flg *170) when Final_dbflag_df_1.DB_name =="db_ccbml_clnrm4" then (DB_check_flg *171) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_ccs" then (DB_check_flg *172) when Final_dbflag_df_1.DB_name =="db_ccb_dsdisc_ws5" then (DB_check_flg *173) when Final_dbflag_df_1.DB_name =="db_rqd_cecl_results" then (DB_check_flg *174) when Final_dbflag_df_1.DB_name =="db_aria_eqr_raw" then (DB_check_flg *175) when Final_dbflag_df_1.DB_name =="institute_consumer" then (DB_check_flg *176) when Final_dbflag_df_1.DB_name =="institute_mortgage" then (DB_check_flg *177) when Final_dbflag_df_1.DB_name =="db_ccbml_deco_cce_pii" then (DB_check_flg *178) when Final_dbflag_df_1.DB_name =="db_rqd_inteldev_sandbox_pii" then (DB_check_flg *179) when Final_dbflag_df_1.DB_name =="db_adi_services" then (DB_check_flg *180) when Final_dbflag_df_1.DB_name =="sys" then (DB_check_flg *181) when Final_dbflag_df_1.DB_name =="db_rqd_research_results" then (DB_check_flg *182) when Final_dbflag_df_1.DB_name =="db_rqd_ckfd_results" then (DB_check_flg *183) when Final_dbflag_df_1.DB_name =="db_opsdata_con_clv" then (DB_check_flg *184) when Final_dbflag_df_1.DB_name =="db_rqd_inv_ca_sandbox_pii" then (DB_check_flg *185) when Final_dbflag_df_1.DB_name =="default" then (DB_check_flg *186) when Final_dbflag_df_1.DB_name =="db_aml_global_curated" then (DB_check_flg *187) when Final_dbflag_df_1.DB_name =="ws_common_stage" then (DB_check_flg *188) when Final_dbflag_df_1.DB_name =="test" then (DB_check_flg *189) when Final_dbflag_df_1.DB_name =="db_audit_common" then (DB_check_flg *190) when Final_dbflag_df_1.DB_name =="db_aml_dataops_refined" then (DB_check_flg *191) when Final_dbflag_df_1.DB_name =="institute_retail_curated" then (DB_check_flg *192) when Final_dbflag_df_1.DB_name =="db_opsdata_raw_d" then (DB_check_flg *193) when Final_dbflag_df_1.DB_name =="db_ccbml_clnrm7" then (DB_check_flg *194) when Final_dbflag_df_1.DB_name =="db_wss_trac" then (DB_check_flg *195) when Final_dbflag_df_1.DB_name =="db_aml_fraud_raw" then (DB_check_flg *196) when Final_dbflag_df_1.DB_name =="institute_archive" then (DB_check_flg *197) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_operations" then (DB_check_flg *198) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_decsci" then (DB_check_flg *199) when Final_dbflag_df_1.DB_name =="db_ccbml_deco_neuro" then (DB_check_flg *200) when Final_dbflag_df_1.DB_name =="db_opsdata_con_ccb" then (DB_check_flg *201) when Final_dbflag_df_1.DB_name =="db_emea_aml_amlfraud_hivedb1" then (DB_check_flg *202) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_test" then (DB_check_flg *203) when Final_dbflag_df_1.DB_name =="db_opsdata_sem_mb" then (DB_check_flg *204) when Final_dbflag_df_1.DB_name =="db_opsdata_con_bb" then (DB_check_flg *205) when Final_dbflag_df_1.DB_name =="db_mrchsvc_msas_tech" then (DB_check_flg *206) when Final_dbflag_df_1.DB_name =="db_rqd_ceclval_source" then (DB_check_flg *207) else (DB_check_flg*0) end as DB_check_flg from Final_dbflag_df_1')    

Final_dbflag_df_different_dbs.write.csv("/tmp/i725369/db_flg_gold_reports/Combined_scenario1_different_db_report_1")

***********************

Final_dbflag_df=spark.sql('select trim(DB_name) as DB_name ,trim(Table_Id) as Table_Id,trim(Table_name) as Table_name,trim(Table_Type) as Table_Type,trim(DEDUP_KEY) as DEDUP_KEY ,trim(COLUMN_COUNT) as COLUMN_COUNT,trim(golden_copy_key) as golden_copy_key ,case when t3_df.grouped_count == t3_df.c1 then (DB_check_flg*0) else DB_check_flg end as DB_check_flg from t3_df')

Final_dbflag_df.registerTempTable('Final_dbflag_df')

Final_dbflag_df_0=spark.sql('select DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,golden_copy_key from Final_dbflag_df where DB_check_flg = 0')

Final_dbflag_df_0.write.csv("/tmp/i725369/db_flg_gold_reports/DUP_TBL_CMBND_MAP_CEA_SC1_RPT")

*****************************************END*****************************************************



*****************************************
mp_c_sc2
*******************************************

df_details = [StructField('DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Table_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('DEDUP_KEY',StringType(),True),StructField('COLUMN_COUNT',StringType(),True),StructField('DB_check_flg',StringType(),True)]
final_details = StructType(fields=df_details)
sc1_final_report_time_map_cea=spark.read.csv('/tmp/i725369/map_cea_output_files/sc1_map_cea_output_07_31/*.csv', schema = final_details)

sc1_final_report_time_map_cea.registerTempTable('sc1_final_report_time_map_cea')
same_db_map_cea=spark.sql('select * from sc1_final_report_time_map_cea where DB_check_flg =0')
different_db_map_cea=spark.sql('select * from sc1_final_report_time_map_cea where DB_check_flg =1')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import col
import pyspark
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import functions as f
from pyspark.sql import types as t 

select a.NAME as DB_NAME,b.TBL_NAME,concat(a.NAME,'@',b.TBL_NAME) as DB_TBL,b.CREATE_TIME from DBS a join TBLS b on a.DB_ID = b.DB_ID

Dum_query:

select a.database_name as DB_NAME,a.tablename as TBL_NAME,concat(a.database_name,'@',a.tablename) as DB_TBL, count(a.ACTION) as Total_Count  from db_bdcs.v_dum360_ccb a where tablename not in('N/A', '') and platform ='Cloudera' and cluster ='MT-DISCOVERY'  and access_allowed_flag = 'Y' group by a.database_name,a.tablename order by Total_Count desc

/tmp/r660737/hive_m3.txt

/tmp/r660737/dum_mt.txt
*************
from pyspark.sql import functions as f

df_hive = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('CREATE_TIME',StringType(),True)]
hive_details = StructType(fields=df_hive)
hive_df=spark.read.option("sep", "\t").csv("/tmp/i725369/map_cea_input_files/subset_files/hive_metastore_mapcea_dbtbl.csv", schema = hive_details)


df_dum = [StructField('DB_NAME',StringType(),True), StructField('TBL_NAME',StringType(),True),StructField('DB_TBL',StringType(),True),StructField('Count',StringType(),True)]
dum_details = StructType(fields=df_dum)
dum_dfdf=spark.read.csv("/tmp/i725369/map_cea_input_files/subset_files/Map_Cea_Dum.csv", schema = dum_details)


innerjoin_DF = dum_dfdf.join(hive_df, trim(dum_dfdf.DB_TBL) == trim( hive_df.DB_TBL)).select(dum_dfdf.DB_NAME,hive_df.DB_TBL,dum_dfdf.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

Left_join_DF = hive_df.join(dum_dfdf, trim(hive_df.DB_TBL) == trim(dum_dfdf.DB_TBL),how ='left').select(hive_df.DB_NAME,hive_df.DB_TBL,hive_df.TBL_NAME,dum_dfdf.Count,hive_df.CREATE_TIME)

remove_null_DF = Left_join_DF.where("Count is null").select(Left_join_DF.DB_NAME,Left_join_DF.DB_TBL,Left_join_DF.TBL_NAME,Left_join_DF.Count,Left_join_DF.CREATE_TIME)

finalDF = innerjoin_DF.union(remove_null_DF)

********************************

same_db_map_cea.registerTempTable('sc1_df')

sc1_final_df = spark.sql("select trim(DB_name) as DB_name,Table_Id,Table_name,Table_Type,DEDUP_KEY,COLUMN_COUNT,concat(DB_name,'@',Table_name) as DB_TBL from sc1_df")


finalDF.registerTempTable('finalDF')
combined_df = spark.sql("select DB_TBL,Count,trim(CREATE_TIME) as CREATE_TIME from finalDF")

Merge_DF = sc1_final_df.join(combined_df, trim(sc1_final_df.DB_TBL) == trim( combined_df.DB_TBL)).select(sc1_final_df.DB_name,sc1_final_df.Table_Id,sc1_final_df.Table_name,sc1_final_df.DEDUP_KEY,sc1_final_df.COLUMN_COUNT,combined_df.Count,combined_df.CREATE_TIME)

returnDF = Merge_DF.fillna({'Count':'0'})

data_df = returnDF.withColumn("Count", returnDF["Count"].cast(IntegerType()))

time_cnv_df = data_df.withColumn("CREATE_TIME", data_df["CREATE_TIME"].cast(IntegerType()))
Hive_new_df=time_cnv_df.withColumn('CREATE_TIME', f.date_format(time_cnv_df.CREATE_TIME.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))

Hive_new_df.registerTempTable('Hive_new_df')

******************map_cea_queries-----
combined_df = spark.sql("select * from Hive_new_df where DB_name in ('a_fox_prd_jds_impaladb','a_fox_prddb','da','da_ai','da_audit','da_cia','da_stg','da_tmp','da_user','db_busbank_sandbox','db_ccbml_opt_icr_pii','db_ccbml_pprd_icr_pii','db_cea_raw','db_map_curated','db_map_refined','db_map_user','db_merlin_buildprd_curated','db_merlin_buildprd_raw','db_merlin_buildprd_refined','db_merlin_curated','db_merlin_raw','db_merlin_refined','db_merlin_sandbox','db_merlin_sandbox_bureau','db_merlin_sandbox_pii','db_opsdata_curated','db_opsdata_raw','db_opsdata_refined','default')")



********************
from pyspark.sql.functions import desc

Sorted_df=combined_df.sort(desc("DEDUP_KEY"),desc("Count"))

final_same_result_df=Sorted_df.dropDuplicates(subset = ['DEDUP_KEY'])

final_same_result_df.registerTempTable('final_same_result_df')
different_db_map_cea.registerTempTable('different_db_map_cea')

same_map_cea_final=spark.sql('select trim(Table_Id)as Table_Id FROM final_same_result_df')
different_map_cea_final=spark.sql('select trim(Table_Id)as Table_Id FROM different_db_map_cea')

final_input_map_cea_sc2=same_map_cea_final.union(different_map_cea_final)

***********************
*********************

def cleancolumns(x):
    firstbrace=x.find('(')
    lastbrace=x.rfind(')')
    cleaneddata=x[firstbrace+1:lastbrace]
    cleanval=cleaneddata.split(',',2)
    column1=cleanval[1]
    datatype=cleanval[2]
    return (cleanval[0],column1,datatype)

combineddf=sc.textFile('/tmp/i725369/map_cea_input_files/combined_data_exclue_sid.txt',4)


combined_map=combineddf.map(cleancolumns)

comb_final_df=spark.createDataFrame(combined_map,('tableid','columnname','datatype'))

combined_master_Df = comb_final_df.withColumn("tableid",regexp_replace("tableid", "L", "")).withColumn("columnname",regexp_replace("columnname", "'", "")).withColumn("datatype",regexp_replace("datatype", "'", ""))
1015551

Tabl_id :1012418


combined_master_Df.registerTempTable('combined_master_Df')

master_inputdf=spark.sql('select trim(tableid)as tableid,trim(columnname)as columnname,trim(datatype)as datatype from combined_master_Df')

final_input_map_cea_sc2.registerTempTable('final_input_map_cea_sc2')
master_inputdf.registerTempTable('master_inputdf')

mt_discover_input_df=spark.sql('select trim(master_inputdf.tableid)as tableid,trim(master_inputdf.columnname)as columnname ,trim(master_inputdf.datatype) as datatype  from final_input_map_cea_sc2 join master_inputdf on final_input_map_cea_sc2.Table_Id== master_inputdf.tableid')

mt_discover_input_df.registerTempTable('mt_discover_input_df')

*********************************

combinedf=spark.sql('select a.tableid as tableid1,b.tableid as tableid2,1 as cnt from mt_discover_input_df a join mt_discover_input_df b where trim(a.columnname)=trim(b.columnname) and a.datatype=b.datatype')

combinedf.registerTempTable('combinedf')
combinedfaggregate=spark.sql('select tableid1,tableid2,count(cnt) as cnt from combinedf group by tableid1,tableid2').cache()
combinedfaggregate.registerTempTable('combinedfaggregate')
combinedfgrp=spark.sql('select count(*) as columncount,trim(tableid)as tableid from mt_discover_input_df group by tableid').cache()
combinedfgrp.show(1)
combinedfgrp.registerTempTable('combinedfgrp')

resultset=spark.sql('select a.tableid1,a.tableid2,round(cnt/columncount*100) as match from combinedfaggregate a join combinedfgrp b on a.tableid1=b.tableid')
resultsetfiltered=resultset.filter((resultset.match > 80)& (resultset.match<100))

resultsetfiltered.registerTempTable('resultsetfiltered')

filteredresultsetfiltereddupstbl1=spark.sql('select a.tableid1,a.tableid2,a.match,b.columncount as tableid1cnt from resultsetfiltered a LEFT join combinedfgrp b  on trim(a.tableid1)=trim(b.tableid) ')
filteredresultsetfiltereddupstbl2=spark.sql('select a.tableid1,a.tableid2,a.match,b.columncount as tableid2cnt from resultsetfiltered a LEFT join combinedfgrp b  on trim(a.tableid2)=trim(b.tableid)')
filteredresultsetfiltereddupstbl1.registerTempTable('table1cnt')
filteredresultsetfiltereddupstbl2.registerTempTable('table2cnt')


finalop=spark.sql('select a.tableid1,a.tableid2,a.match,a.tableid1cnt,b.tableid2cnt from table1cnt a left join table2cnt b on a.tableid1=b.tableid1 and a.tableid2=b.tableid2 and a.match=b.match' )
finalop.registerTempTable('finalop')

finalopreport=spark.sql('select tableid1,tableid2,match, case when (tableid1cnt > tableid2cnt) then round(tableid2cnt/tableid1cnt*100) else  round(tableid1cnt/tableid2cnt*100 )end as columcntpercentage from finalop')

finalopreport.registerTempTable('test')
df=spark.sql('select tableid1,tableid2,match,columcntpercentage,1 as flag  from test')
df.registerTempTable('df')
df1=spark.sql('select tableid1,tableid2,match,columcntpercentage,case when (columcntpercentage < 50 ) then flag  else (flag*0)end as flag_key from df')

df_details = [StructField('Champion_DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Champion_Tbl_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details)
db_details=spark.read.csv('/tmp/i725369/map_cea_input_files/db_information_details.txt', schema = final_details)

df_detail = db_details.withColumn("Champion_DB_name",regexp_replace("Champion_DB_name", "'", "")).withColumn("Champion_Tbl_name",regexp_replace("Champion_Tbl_name", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("Table_Id",regexp_replace("Table_Id", "L", ""))


parent_df = df1.join(df_detail,trim(df1.tableid1) == trim(df_detail.Table_Id)).select(df_detail.Champion_DB_name,df1.tableid1,df_detail.Champion_Tbl_name,df1.tableid2,df1.match,df1.columcntpercentage,df1.flag_key)

df_details_1 = [StructField('Challenger_DB_name',StringType(),True), StructField('Table_Id',StringType(),True),StructField('Challenger_Tbl_name',StringType(),True),StructField('Table_Type',StringType(),True),StructField('COLUMN_COUNT',StringType(),True)]
final_details = StructType(fields=df_details_1)
db_details1=spark.read.csv('/tmp/i725369/map_cea_input_files/db_information_details.txt', schema = final_details)

df_detail1= db_details1.withColumn("Challenger_DB_name",regexp_replace("Challenger_DB_name", "'", "")).withColumn("Challenger_Tbl_name",regexp_replace("Challenger_Tbl_name", "'", "")).withColumn("COLUMN_COUNT",regexp_replace("COLUMN_COUNT", "L", "")).withColumn("Table_Type",regexp_replace("Table_Type", "'", "")).withColumn("Table_Id",regexp_replace("Table_Id", "L", ""))

children_df = df1.join(df_detail1,trim(df1.tableid2) == trim(df_detail1.Table_Id)).select(df1.tableid1,df1.tableid2,df_detail1.Challenger_Tbl_name,df_detail1.Challenger_DB_name,df1.match,df1.columcntpercentage,df1.flag_key)


parent_df.registerTempTable("Parent")
children_df.registerTempTable("Child")

df_result  = sqlContext.sql("select a.Champion_DB_name,a.Champion_Tbl_name,b.Challenger_DB_name,b.Challenger_Tbl_name,b.match,b.flag_key from Parent a join child b on a.tableid1= b.tableid1 and a.tableid2=b.tableid2")

df_result.show(15, truncate = False)

df_result.write.csv("/tmp/i725369/map_cea_output_files/sc2_map_cea_output_08_0")

**************************************************END**********************************
