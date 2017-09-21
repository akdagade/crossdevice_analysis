## Audit unique user count
## Data for 8th Sept
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select count(*) as impcount, count(distinct uid) as dist_uid, sum(if(uid is null or uid='' or uid='TCNE' or uid='tcne' or uid='NOUSERGUIDSPECIFIED' or uid like '%null%',0,1)) as uid_count from raw_cduid_407;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_uid_count_1.tsv &

## Data for 14th Sept
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select count(*) as impcount, count(distinct uid) as dist_uid, sum(if(uid is null or uid='' or uid='TCNE' or uid='tcne' or uid='NOUSERGUIDSPECIFIED' or uid like '%null%',0,1)) as uid_count from raw_cduid_407_2;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_uid_count_2.tsv &

~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Get data from raw logger data
## raw_uid_data - Pubmatic uid, cd uid for 8th Sept ---- raw_cduid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407 row format delimited fields terminated by '\t' as  select \`date\`,hour,imprid,browser,uid,cduid,it,adt,wops,svr,dc,pid,sid,adid,wcid,pb,wadnid,mob.id as mob_id, mob.pi as mob_pi, mob.ut as mob_ut, mob.uh as mob_uh  from raw_logger_data where \`date\`=08 and month=09 and year=2017;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_tab.tsv &

## raw_uid_data - Pubmatic uid, cd uid for 14th Sept ---- raw_cduid_407_2
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_2 row format delimited fields terminated by '\t' as  select \`date\`,hour,imprid,browser,uid,cduid,it,adt,wops,svr,dc,pid,sid,adid,wcid,pb,wadnid,mob.id as mob_id, mob.pi as mob_pi, mob.ut as mob_ut, mob.uh as mob_uh  from raw_logger_data where \`date\`=14 and month=09 and year=2017;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_tab_2.tsv &

~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Get cduid in same row
## Explode raw_cduid_407 ---- raw_cduid_407_explode
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_explode row format delimited fields terminated by '\t' as select \`date\`,hour,imprid,uid,it,cdu.dsp_id as cdu_dspid, cdu.cmpg_id as cdu_cmpgid, cdu.uid as cdu_uid, mob_id,mob_pi,mob_ut,mob_uh  from raw_cduid_407 LATERAL VIEW explode(cduid) cduidExp as cdu;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_explode_tab.tsv &

## Get unique dsp campaign from raw_cduid_407_explode
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select distinct cdu_cmpgid from raw_cduid_407_explode;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_unique_cmp.tsv &

cdu_cmpgid
NULL
22967
22918
22968
22968
16735
16736
15546
22947
Time taken: 1354.71 seconds, Fetched: 8 row(s)

+-------+-------------------+----------------------------+
| id    | demand_partner_id | name                       |
+-------+-------------------+----------------------------+
| 15546 |                18 | RocketFuel                 |
| 22947 |                18 | RocketFuel                 |
| 16735 |                27 | Media Math                 |
| 16736 |                27 | Media Math                 |
| 22918 |               377 | The Trade Desk             |
| 22967 |              1066 | Cross Device Test LiveRamp |
| 22968 |              1067 | Cross Device Test Neustar  |
+-------+-------------------+----------------------------+

~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##Create liveramp-rocketfuel uid dump
##created table - liveramp_rfuid_407_compressed
drop table liveramp_rfuid_407_compressed;

CREATE EXTERNAL TABLE `liveramp_rfuid_407_compressed`
(rf_uid string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/RF/*full*' into table liveramp_rfuid_407_compressed;

#!!#Uncompress data into another table liveramp_rfuid_407_compressed ---- liveramp_rfuid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_rfuid_407 row format delimited fields terminated by '\t' as select * from liveramp_rfuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_rfuid_407_tab.tsv &

##created table - liveramp_idfa_407_compressed
CREATE EXTERNAL TABLE `liveramp_idfa_407_compressed`
(ios_id string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/IDFA/*' into table liveramp_idfa_407_compressed;

##created table - liveramp_aaid_407_compressed
CREATE EXTERNAL TABLE `liveramp_aaid_407_compressed`
(and_id string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/AAID/*' into table liveramp_aaid_407_compressed;

~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##Pubmatic side tables
##Create Liveramp to device id mapping raw_cduid_407_explode ---- raw_cduid_407_mobid_lr

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_mobid_lr row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.uid as pubmatic_uid, a.it, b.cdu_dspid, b.cdu_cmpgid, b.cdu_uid as liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407 a left outer join raw_cduid_407_explode b on a.imprid=b.imprid where b.cdu_cmpgid=22967 and b.cdu_dspid=1066;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_mobid_lr_tab.tsv &