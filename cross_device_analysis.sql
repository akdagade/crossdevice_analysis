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

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select \`date\`,hour,imprid, trim(concat_ws('',collect_set(cdu_uid)) as combi from raw_cduid_407_explode tablesample (1 PERCENT) a group by \`date\`,hour,imprid limit 100;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/test_407_clean.tsv &

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

load data inpath '/test_dee/407/RF/' into table liveramp_rfuid_407_compressed;
load data inpath '/test_dee/407/RF/Pubmatic_LR-7232_INDV-IDGraph_IDLT_RocketFuel_20170908_195315.txt.0001_incr.gz' into table liveramp_rfuid_407_compressed;
load data inpath '/test_dee/407/RF/Pubmatic_LR-7232_INDV-IDGraph_IDLT_RocketFuel_20170908_195315.txt.0002_incr.gz' into table liveramp_rfuid_407_compressed;


#!!#Uncompress data into another table liveramp_rfuid_407_compressed ---- liveramp_rfuid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table liveramp_rfuid_407;create table liveramp_rfuid_407 row format delimited fields terminated by '\t' as select * from liveramp_rfuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_rfuid_407_tab.tsv &


##created table - liveramp_idfa_407_compressed
CREATE EXTERNAL TABLE `liveramp_idfa_407_compressed`
(ios_id string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/IDFA/*' into table liveramp_idfa_407_compressed;

#!!#Uncompress data into another table liveramp_idfa_407_compressed ---- liveramp_idfa_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_idfa_407 row format delimited fields terminated by '\t' as select * from liveramp_idfa_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_idfa_407_tab.tsv &


##created table - liveramp_aaid_407_compressed
CREATE EXTERNAL TABLE `liveramp_aaid_407_compressed`
(and_id string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/AAID/*' into table liveramp_aaid_407_compressed;

#!!#Uncompress data into another table liveramp_aaid_407_compressed ---- liveramp_aaid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_aaid_407 row format delimited fields terminated by '\t' as select * from liveramp_aaid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_aaid_407_tab.tsv &


##created table - liveramp_mmuid_407_compressed
drop table liveramp_mmuid_407_compressed;

CREATE EXTERNAL TABLE `liveramp_mmuid_407_compressed`
(mm_uid string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/MM/' into table liveramp_mmuid_407_compressed;


#!!#Uncompress data into another table liveramp_rfuid_407_compressed ---- liveramp_rfuid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407 row format delimited fields terminated by '\t' as select * from liveramp_mmuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_mmuid_407_tab.tsv &


#No of impression to DSP
nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;
create table cmpg_407 row format delimited fields terminated by '\t' as  select \`date\`,hour,pid,wcid,browser,wbid,wadnid,verified,it,imprid,cmpg,mob.id as mob_id, mob.pi as mob_pi, mob.ut as mob_ut, mob.uh as mob_uh, mob.os as mob_os from raw_logger_tracker_data where year=2017 and month=09 and \`date\`=08;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/cmpg_407_tab.tsv &
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

##Pubmatic side tables
##Create Liveramp to device id mapping raw_cduid_407_explode ---- raw_cduid_407_mobid_lr
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_mobid_lr row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.uid as pubmatic_uid, a.it, b.cdu_dspid, b.cdu_cmpgid, b.cdu_uid as liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407 a left outer join raw_cduid_407_explode b on a.imprid=b.imprid where b.cdu_cmpgid=22967 and b.cdu_dspid=1066;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_mobid_lr_tab.tsv &	

## Creating preusable table for entire analysis raw_cduid_407_explode ---- raw_cduid_407_preuseable

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_preuseable row format delimited fields terminated by '\t' as select \`date\`,hour,imprid,uid,it,
if(cdu_dspid=18 and cdu_cmpgid=15546 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as RF_15546_uid,
if(cdu_dspid=18 and cdu_cmpgid=22947 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null ,cdu_uid,'') as RF_22947_uid,
if(cdu_dspid=27 and cdu_cmpgid=16735 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as MM_16735_uid,
if(cdu_dspid=27 and cdu_cmpgid=16736 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as MM_16736_uid,
if(cdu_dspid=377 and cdu_cmpgid=22918 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as TTD_uid,
if(cdu_dspid=1066 and cdu_cmpgid=22967 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as LR_uid,
if(cdu_dspid=1067 and cdu_cmpgid=22968 and cdu_uid!='' and cdu_uid not like '%null%' and cdu_uid is not null,cdu_uid,'') as Neustar_uid,
 mob_id,mob_pi,mob_ut,mob_uh from raw_cduid_407_explode;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_preuseable_tab.tsv &

## Creating usable table for entire analysis raw_cduid_407_preusable ---- raw_cduid_407_useable

 nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_useable row format delimited fields terminated by '\t' as select \`date\`,hour,imprid,uid,it, trim(concat_ws('',collect_set(RF_15546_uid))) as RF_15546_uid, trim(concat_ws('',collect_set(RF_22947_uid))) as RF_22947_uid, trim(concat_ws('',collect_set(MM_16735_uid))) as MM_16735_uid, trim(concat_ws('',collect_set(MM_16736_uid))) as MM_16736_uid,trim(concat_ws('',collect_set(TTD_uid))) as TTD_uid,trim(concat_ws('',collect_set(LR_uid))) as LR_uid, trim(concat_ws('',collect_set(Neustar_uid))) as Neustar_uid, mob_id,mob_pi,mob_ut,mob_uh from raw_cduid_407_preuseable group by \`date\`,hour,imprid,uid,it,mob_id,mob_pi,mob_ut,mob_uh;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_useable_tab.tsv &

## Creating usable table for entire analysis raw_cduid_407_useable ---- raw_cduid_407_data

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_data row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.uid as pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, b.RF_15546_uid, b.RF_22947_uid, b.MM_16736_uid, b.MM_16735_uid, b.TTD_uid, b.LR_uid, b.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407 a left outer join raw_cduid_407_useable b on a.imprid=b.imprid;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_data_tab.tsv &

~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Liveramp RF analysis
#PubMatic ID is present but no RF ID and LiveRamp has the user details for RF.

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table pubmatic_lr_rf_407_1 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.LR_uid, collect_set(b.rf_uid) as rf_uid_list, b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407_data a join liveramp_rfuid_407 b on a.LR_uid=b.liveramp_id where pubmatic_uid is not null and pubmatic_uid!='' and pubmatic_uid!='NOUSERGUIDSPECIFIED' and pubmatic_uid not like 'TCNE' and b.rf_uid !='' and b.rf_uid is not null and (a.RF_15546_uid is null or a.RF_15546_uid='') and (a.RF_22947_uid is null or a.RF_22947_uid='') group by a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.LR_uid,b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/pubmatic_lr_rf_407_1_tab.tsv &

#LR RF match rate for mobile app
hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select count(*) as count from pubmatic_lr_rf_407_1 where it='ma' or it='MA'" -hiveconf mapreduce.job.queuename=AdhocMl 


#we will need Total no. of impression for a day, how many we had a match for and how many did LR provide a match for.
#total requests 37919704260
hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select count(*) as count from raw_cduid_407_data where pubmatic_uid!='' and pubmatic_uid not like 'TCNE' and pubmatic_uid not like 'tcne' and pubmatic_uid not like 'NOUSERGUIDSPECIFIED' and ((RF_15546_uid!='' and RF_15546_uid is not null and RF_15546_uid not like 'NULL') OR (RF_22947_uid!='' and RF_22947_uid is not null and RF_22947_uid not like 'NULL'));" -hiveconf mapreduce.job.queuename=AdhocMl 

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Liveramp MM analysis
#PubMatic ID is present but no MM ID and LiveRamp has the user details for MM.

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table pubmatic_lr_mm_407_1 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.MM_16735_uid, a.MM_16736_uid, a.LR_uid, collect_set(b.mm_uid) as mm_uid_list, b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407_data a join liveramp_mmuid_407 b on a.LR_uid=b.liveramp_id where pubmatic_uid is not null and pubmatic_uid!='' and pubmatic_uid!='NOUSERGUIDSPECIFIED' and pubmatic_uid not like 'TCNE' and b.mm_uid !='' and b.mm_uid is not null and (a.MM_16736_uid is null or a.MM_16736_uid='') and (a.MM_16735_uid is null or a.MM_16735_uid='') group by a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.MM_16736_uid, a.MM_16735_uid, a.LR_uid,b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/pubmatic_lr_mm_407_1_tab.tsv &

#LR MM match rate for mobile app
hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it,count(*) as count from pubmatic_lr_mm_407_1 group by it" -hiveconf mapreduce.job.queuename=AdhocMl 


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it,count(*) as count from raw_cduid_407_data where pubmatic_uid!='' and pubmatic_uid not like 'TCNE' and pubmatic_uid not like 'tcne' and pubmatic_uid not like 'NOUSERGUIDSPECIFIED' and ((MM_16735_uid!='' and MM_16735_uid is not null and MM_16735_uid not like 'NULL') OR (MM_16736_uid!='' and MM_16736_uid is not null and MM_16736_uid not like 'NULL')) group by it;" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/pub_mm_match.tsv &