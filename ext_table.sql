
	1. Create External Table for liveramp_rfuid_407

CREATE EXTERNAL TABLE `liveramp_rfuid_407_ext`(
  `date` int, 
  `hour` int, 
  `imprid` string, 
  `pubmatic_uid` string, 
  `it` string, 
  `browser` string, 
  `wops` int, 
  `dc` string, 
  `svr` string, 
  `rf_15546_uid` string, 
  `rf_22947_uid` string, 
  `mm_16736_uid` string, 
  `mm_16735_uid` string, 
  `ttd_uid` string, 
  `lr_uid` string, 
  `neustar_uid` string, 
  `mob_id` string, 
  `mob_pi` string, 
  `mob_ut` int, 
  `mob_uh` int, 
  `b_liveramp_id` string)
PARTITIONED BY ( 
  `file` int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

load data inpath "hdfs://Matrix-Aggr/data1/apps/hive/warehouse/raw_lrapp_407/000000_0" into table liveramp_rfuid_407_ext PARTITION(`file`=0);
load data inpath "hdfs://Matrix-Aggr/data1/apps/hive/warehouse/raw_lrapp_407/000001_0" into table liveramp_rfuid_407_ext PARTITION(`file`=1);
load data inpath "hdfs://Matrix-Aggr/data1/apps/hive/warehouse/raw_lrapp_407/000002_0" into table liveramp_rfuid_407_ext PARTITION(`file`=2);
load data inpath "hdfs://Matrix-Aggr/data1/apps/hive/warehouse/raw_lrapp_407/000003_0" into table liveramp_rfuid_407_ext PARTITION(`file`=3);
.
.
.
load data inpath "hdfs://Matrix-Aggr/data1/apps/hive/warehouse/raw_lrapp_407/000049_0" into table liveramp_rfuid_407_ext PARTITION(`file`=49);

2. Create External Table for liveramp_mmuid_407

CREATE TABLE `liveramp_mmuid_407_ext`(
  `mm_uid` string, 
  `liveramp_id` string)
PARTITIONED BY ( 
  `file` int)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

load data inpath 'hdfs://Matrix-Aggr/data1/apps/hive/warehouse/liveramp_mmuid_407/000000_0' into table liveramp_mmuid_407_ext partition(`file`=0);
load data inpath 'hdfs://Matrix-Aggr/data1/apps/hive/warehouse/liveramp_mmuid_407/000001_0' into table liveramp_mmuid_407_ext partition(`file`=1);
load data inpath 'hdfs://Matrix-Aggr/data1/apps/hive/warehouse/liveramp_mmuid_407/000002_0' into table liveramp_mmuid_407_ext partition(`file`=2);
.
.
.
load data inpath 'hdfs://Matrix-Aggr/data1/apps/hive/warehouse/liveramp_mmuid_407/000086_0' into table liveramp_mmuid_407_ext partition(`file`=86);

  3. mmuid bkp

  nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407_bkp row format delimited fields terminated by '\t' as select * from liveramp_mmuid_407_ext;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_mmuid_407_bkp.tsv &


  4. Try 1

nohup hive -e"set hive.cli.print.header=true;SET hive.exec.max.created.files=250000;drop table raw_lrapp_mm_407;create table raw_lrapp_mm_407_ext row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.mm_uid  from raw_lrapp_407_ext a join liveramp_mmuid_407_ext  b on a.b_liveramp_id=b.liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_ext_tab.tsv &

