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


#!!#Uncompress data into another table liveramp_mmuid_407_compressed ---- liveramp_mmuid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407 row format delimited fields terminated by '\t' as select * from liveramp_mmuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_mmuid_407_tab.tsv &


##created table - liveramp_ttduid_407_compressed
drop table liveramp_ttduid_407_compressed;

CREATE EXTERNAL TABLE `liveramp_ttduid_407_compressed`	
(ttd_uid string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/TTD/' into table liveramp_ttduid_407_compressed;


#!!#Uncompress data into another table liveramp_ttduid_407_compressed ---- liveramp_ttduid_407
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_ttduid_407 row format delimited fields terminated by '\t' as select * from livx`ramp_ttduid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_ttduid_407_tab.tsv &


#No of impression to DSP
nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;
create table cmpg_407 row format delimited fields terminated by '\t' as  select \`date\`,hour,pid,wcid,browser,wbid,wadnid,verified,it,imprid,cmpg,mob.id as mob_id, mob.pi as mob_pi, mob.ut as mob_ut, mob.uh as mob_uh, mob.os as mob_os from raw_logger_tracker_data where year=2017 and month=09 and \`date\`=08;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/cmpg_407_tab.tsv &

#RF
nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it, sum(if(array_contains(cmpg.id,15546) or array_contains(cmpg.id,22947),1,0)) as RocketFuel, sum(if(array_contains(cmpg.id,16735) or array_contains(cmpg.id,16735),1,0)) as MediaMath, sum(if(array_contains(cmpg.id,22918),1,0)) as TradeDesk from cmpg_407 group by it;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/cmpg_407_data.tsv &

~~~~~~~~~

CREATE EXTERNAL TABLE `liveramp_dev_407`
(dev_id string,
liveramp_id string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/dev/1' into table liveramp_dev_407;
load data inpath '/test_dee/407/dev/2' into table liveramp_dev_407;
#load data inpath '/test_dee/407/AAID/' into table liveramp_dev_407_compressed;
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
hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select count(*) as count from pubmatic_lr_mm_407_1 group by it" -hiveconf mapreduce.job.queuename=AdhocMl 


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it,count(*) as count from raw_cduid_407_data where pubmatic_uid!='' and pubmatic_uid not like 'TCNE' and pubmatic_uid not like 'tcne' and pubmatic_uid not like 'NOUSERGUIDSPECIFIED' and ((MM_16735_uid!='' and MM_16735_uid is not null and MM_16735_uid not like 'NULL') OR (MM_16736_uid!='' and MM_16736_uid is not null and MM_16736_uid not like 'NULL')) group by it;" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/pub_mm_match.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select pid,it,count(*) as count from pubmatic_lr_mm_407_1 where pid in (37855, 137711, 154037) group by it" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/lr_mm_3_pub_data.tsv &

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Liveramp TTD analysis
#PubMatic ID is present but no TTD ID and LiveRamp has the user details for TTD.

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table pubmatic_lr_ttd_407_1 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.TTD_uid, a.LR_uid, collect_set(b.ttd_uid) as ttd_uid_list, b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407_data a join liveramp_ttduid_407 b on a.LR_uid=b.liveramp_id where pubmatic_uid is not null and pubmatic_uid!='' and pubmatic_uid!='NOUSERGUIDSPECIFIED' and pubmatic_uid not like 'TCNE' and b.ttd_uid !='' and b.ttd_uid is not null and (a.TTD_uid is null or a.TTD_uid='') group by a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.TTD_uid, a.LR_uid,b.liveramp_id, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/pubmatic_lr_ttd_407_1_tab.tsv &

#LR TTD match rate for mobile app
hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it,count(*) as count from pubmatic_lr_ttd_407_1 group by it" -hiveconf mapreduce.job.queuename=AdhocMl 


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select it,count(*) as count from raw_cduid_407_data where pubmatic_uid!='' and pubmatic_uid not like 'TCNE' and pubmatic_uid not like 'tcne' and pubmatic_uid not like 'NOUSERGUIDSPECIFIED' and TTD_uid!='' and TTD_uid is not null and TTD_uid not like 'NULL' group by it;" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/pub_ttd_match.tsv &


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Liveramp app analysis	

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_data row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.uid as pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, b.RF_15546_uid, b.RF_22947_uid, b.MM_16736_uid, b.MM_16735_uid, b.TTD_uid, b.LR_uid, b.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407 a left outer join raw_cduid_407_useable b on a.imprid=b.imprid;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_data_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_lrapp_407 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, b.liveramp_id as b_liveramp_id  from raw_cduid_407_data a join liveramp_dev_407 b on a.mob_pi=b.dev_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_tab.tsv &

liveramp_mmuid_407
hdfs://Matrix-Aggr/data1/apps/hive/warehouse/liveramp_mmuid_407

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_lrapp_407_orc stored as orc as select * from raw_lrapp_407;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_orc.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407_compressed_orc stored as orc as select * from liveramp_mmuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_orc.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_lrapp_407_orc_snappy stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY') as select * from raw_lrapp_407;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_orc_snappy.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407_compressed_orc_snappy stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY') as select * from liveramp_mmuid_407_compressed;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_orc_snappy.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table liveramp_rfuid_407_orc_snappy stored as orc TBLPROPERTIES ('orc.compress'='SNAPPY') as select * from liveramp_rfuid_407;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_orc_snappy.tsv &

nohup hive -e"set hive.cli.print.header=true;SET hive.exec.max.created.files=250000;drop table raw_lrapp_mm_407;create table raw_lrapp_mm_407 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.mm_uid  from raw_lrapp_407 tablesample(50 PERCENT) a join liveramp_mmuid_407_compressed  b on a.b_liveramp_id=b.liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_tab_1.tsv &

application_1502886522713_303293

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407;create table raw_lrapp_rf_407 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407 a join liveramp_rfuid_407 b on a.b_liveramp_id=b.liveramp_id where b.rf_uid is not null and rf_uid!='';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407_tst;create table raw_lrapp_rf_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407_orc a join liveramp_rfuid_407_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.rf_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tst_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000; drop table raw_lrapp_rf_407_tst_2;create table raw_lrapp_rf_407_tst_2 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh from raw_lrapp_407_orc a where b_liveramp_id in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tst_tab_2.tsv &

collect_list

liveramp_rfuid_407

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;create table liveramp_rfuid_407_dedup row format delimited fields terminated by '\t' as select liveramp_id, collect_list(rf_uid) as rf_uid from liveramp_rfuid_407 where rf_uid!='' and rf_uid is not null and rf_uid not like '%null%' and rf_uid!='0' group by liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_rfuid_407_dedup_tab.tsv &

 hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;select * from liveramp_rfuid_407_dedup where liveramp_id in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl 

'XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug'
application_1502886522713_303442

nohup hive -e"set hive.cli.print.header=true;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407;create table raw_lrapp_rf_407 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.pubmatic_uid, a.it, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid,a.LR_uid, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407 a join liveramp_rfuid_407 b on a.b_liveramp_id=b.liveramp_id where b.rf_uid is not null and rf_uid!='';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tab.tsv &

~~~~~~ orc SNAPPY


nohup hive -e"set hive.cli.print.header=true;set hive.vectorized.execution.enabled = true;set hive.vectorized.execution.reduce.enabled = true;SET hive.exec.max.created.files=250000;create table raw_lrapp_rf_407_snappy row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407_orc_snappy a join liveramp_rfuid_407_orc_snappy b on a.b_liveramp_id=b.liveramp_id where b.rf_uid is not null and rf_uid!='';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tab.tsv &

application_1502886522713_303442

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;set hive.vectorized.execution.enabled = true;set hive.vectorized.execution.reduce.enabled = true;create table raw_lrapp_mm_407_snappy row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.mm_uid  from raw_lrapp_407_orc_snappy a join liveramp_mmuid_407_compressed_orc_snappy b on a.b_liveramp_id=b.liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_tab_snappy.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;create table liveramp_mmuid_407_compressed_orc_snappy_dedup row format delimited fields terminated by '\t' as select liveramp_id, collect_list(mm_uid) as mm_uid from liveramp_mmuid_407_compressed_orc_snappy where mm_uid!='' and mm_uid is not null and mm_uid not like '%null%' and mm_uid!='0' group by liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_mmuid_407_compressed_orc_snappy_dedup_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407_tst;create table raw_lrapp_mm_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.mm_uid  from raw_lrapp_407_orc a join liveramp_mmuid_407_compressed_orc_snappy_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.mm_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_tst_tab.tsv &	

~~~~~~
only lrid

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_lrapp_407_onlylr row format delimited fields terminated by '\t' as select b_liveramp_id from raw_lrapp_407;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_onlylr_tab.tsv &

nohup hive -e"set hive.cli.print.header=true;set hive.vectorized.execution.enabled = true;set hive.vectorized.execution.reduce.enabled = true;SET hive.exec.max.created.files=250000; select count(*) from raw_lrapp_407_onlylr a join liveramp_rfuid_407 b on a.b_liveramp_id=b.liveramp_id where b.rf_uid is not null and b.rf_uid!='';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_data.tsv &
~~~~ lrapp ttd



nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;create table liveramp_ttduid_407_dedup row format delimited fields terminated by '\t' as select liveramp_id, collect_list(ttd_uid) as ttd_uid from liveramp_ttduid_407 where ttd_uid!='' and ttd_uid is not null and ttd_uid not like '%null%' and ttd_uid!='0' group by liveramp_id;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/liveramp_ttduid_407_dedup_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_ttd_407_tst;create table raw_lrapp_ttd_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.ttd_uid as b_ttd_uid  from raw_lrapp_407_orc a join liveramp_ttduid_407_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.ttd_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_ttd_tst_tab.tsv &


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#########################################

Tapad analysis

CREATE EXTERNAL TABLE `tapad_graph_compressed`	
(tapad_string string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';

load data inpath '/test_dee/407/tapad/' into table tapad_graph_compressed;

nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;add FILE /data1/MLAA/tapad/parseTapad.py;set mapreduce.job.queuename=AdhocMl;create table tapad_graph_parsed row format delimited fields terminated by '\t' as SELECT TRANSFORM (tapad_string) using 'python parseTapad.py' as (clusterid,pbm,rf,ttd,andid,idfa,sh1dfa,md5idfa) from tapad_graph_compressed;" > /tmp/tapad_graph_parsed_tab.tsv &

trim(concat_ws('',collect_set(cdu_uid))

nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;set mapreduce.job.queuename=AdhocMl;create table tapad_graph_parsed_unprocessed1 row format delimited fields terminated by '\t' as SELECT clusterid,split(pbm,','),rf,ttd,andid,idfa,sh1dfa,md5idfa from tapad_graph_parsed;" > /tmp/tapad_graph_parsed_unprocessed1_tab.tsv &

nohup hive -e"SET mapred.job.priority=VERY_HIGH;set hive.cli.print.header=true;set hive.exec.reducers.max=50;set mapreduce.job.queuename=AdhocMl;DROP TABLE tapad_graph;create table tapad_graph_final row format delimited fields terminated by '\t' as SELECT p as pubmatic_uid, split(trim(concat_ws(',',collect_set(clusterid))),',') as clusterid, split(trim(concat_ws(',',collect_set(rf))),',') as rf, split(trim(concat_ws(',',collect_set(ttd))),',') as ttd, split(trim(concat_ws(',',collect_set(andid))),',') as andid, split(trim(concat_ws(',',collect_set(idfa))),',') as idfa, split(trim(concat_ws(',',collect_set(sh1dfa))),',') as sh1dfa, split(trim(concat_ws(',',collect_set(md5idfa))),',') as md5idfa from tapad_graph_parsed_unprocessed1 LATERAL VIEW explode(\`_c1\`) pbmEXP as p group by p;" > /tmp/tapad_graph_final_tab.tsv &

~~~~~~~~~~~
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table raw_cduid_407_data row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.uid as pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, b.RF_15546_uid, b.RF_22947_uid, b.MM_16736_uid, b.MM_16735_uid, b.TTD_uid, b.LR_uid, b.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh  from raw_cduid_407 a left outer join raw_cduid_407_useable b on a.imprid=b.imprid;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_cduid_407_data_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table tapad_dsp_407_tab;create table tapad_dsp_407_tab row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.TTD_uid, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, b.rf,b.ttd, size(b.rf) as rf_size, size(b.ttd) as rf_ttd from raw_cduid_407_data a join tapad_graph_final b on a.pubmatic_uid=b.pubmatic_uid where a.pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and a.pubmatic_uid is not null;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select sum(if(rf_size<1,0,1)) as rf_count, sum(if(rf_ttd<1,0,1))  as ttd_count from tapad_dsp_407_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_data.tsv &

(RF_15546_uid is null or RF_15546_uid='' or  RF_15546_uid like 'null') and (RF_22947_uid is null or RF_22947_uid='' or RF_22947_uid like 'null')
(ttd_uid is null or ttd_uid='') 
	

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select sum(if((RF_15546_uid is null or RF_15546_uid='' or  RF_15546_uid like 'null') and (RF_22947_uid is null or RF_22947_uid='' or RF_22947_uid like 'null') and rf_size>0,1,0)) as rf_count, sum(if((ttd_uid is null or ttd_uid='' or ttd_uid like 'null') and rf_ttd>0,1,0))  as ttd_count from tapad_dsp_407_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_inc_data.tsv &

	~~~~~~~~~~~~~!!!!!!!!!!!!!!!!!!!!!~~~~~~~~~~~~~~~
	Testing

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


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table tapad_dsp_407_tab row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.TTD_uid, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, b.rf,b.ttd, size(b.rf) as rf_size, size(b.ttd) as rf_ttd from raw_cduid_407_data a join tapad_graph_final b on a.pubmatic_uid=b.pubmatic_uid;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407;create table raw_lrapp_rf_407 row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407 a join liveramp_rfuid_407 b on a.b_liveramp_id=b.liveramp_id where b.rf_uid is not null and rf_uid!='';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tab.tsv &

#######
#Skiewness analysis

raw_lrapp_407

	nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select b_liveramp_id, count(*) as uid_count from raw_lrapp_407_orc group by b_liveramp_id order by uid_count desc limit 1000;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/skiewness_blrid_data.tsv &


liveramp_dev_407

	nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select dev_id, count(*) as uid_count from liveramp_dev_407 group by dev_id order by uid_count desc limit 1000;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/skiewness_lrdevid_data.tsv &


raw_cduid_407_data

	nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select mob_pi, count(*) as uid_count from raw_cduid_407_data where it='ma' or it='MA' group by mob_pi order by uid_count desc limit 1000;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/skiewness_cduid_data.tsv &

	nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select count(*) as dev_count from raw_cduid_407_data where (it='ma' or it='MA') and mob_pi is not null and mob_pi!='' and mob_pi!='0' and mob_pi not like '%null%';" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/devid_count_data.tsv &

###########

##3 publisher analysis

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table 3pub_raw_cduid_407 row format delimited fields terminated by '\t' as  select \`date\`,hour,imprid,pid from raw_cduid_407 where pid in (37855, 137711, 154037);" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/3pub_raw_cduid_407_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select pid,count(*) as count from 3pub_raw_cduid_407 group by pid" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/3pub_lr_count.tsv &


pid	count
37855	40453124
137711	4710452100
154037	1597762165

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select b.pid,it,count(*) as count from pubmatic_lr_rf_407_1 a join 3pub_raw_cduid_407 b on a.imprid=b.imprid group by pid,it" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/3pub_lr_rf.tsv &

b.pid	it	count
37855	ds	25846
37855	mw	4580
137711	ds	3912055
137711	mw	3471860
154037	ds	890433
154037	mw	440589


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select b.pid,it,count(*) as count from pubmatic_lr_mm_407_1 a join 3pub_raw_cduid_407 b on a.imprid=b.imprid group by pid,it" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/3pub_lr_mm.tsv &

b.pid	it	count
37855	ds	7130
37855	mw	733
137711	ds	2046115
137711	mw	552195
154037	ds	164618
154037	mw	141124

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;select b.pid,it,count(*) as count from pubmatic_lr_ttd_407_1 a join 3pub_raw_cduid_407 b on a.imprid=b.imprid group by pid,it" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/3pub_lr_ttd.tsv &

b.pid	it	count
37855	ds	9435
37855	mw	643
137711	ds	4662214
137711	mw	862890
154037	ds	304781
154037	mw	90896


~~~~~~~~~~~~~~~~ARR-577 analysis

liveramp_dev_407
liveramp_rfuid_407
liveramp_mmuid_407_compressed
liveramp_ttduid_407

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table arr_577_rf_dev;create table arr_577_rf_dev row format delimited fields terminated by '\t' as select dev_id,count(*) as count from liveramp_dev_407 a join liveramp_rfuid_407 b on a.liveramp_id=b.liveramp_id where rf_uid!='' and rf_uid is not null and rf_uid not like 'null' and a.liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug') group by dev_id" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_rf_dev_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table arr_577_rf_dev_2;create table arr_577_rf_dev_2 row format delimited fields terminated by '\t' as select dev_id,count(*) as count from liveramp_dev_407 where liveramp_id in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug') group by dev_id" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_rf_dev_tab_2.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_mm_dev row format delimited fields terminated by '\t' as select dev_id,count(*) as count from liveramp_dev_407 a join liveramp_mmuid_407_compressed b on a.liveramp_id=b.liveramp_id where mm_uid!='' and mm_uid is not null and mm_uid not like 'null' and a.liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug') group by dev_id" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_mm_dev_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_ttd_dev row format delimited fields terminated by '\t' as select dev_id,count(*) as count from liveramp_dev_407 a join liveramp_ttduid_407 b on a.liveramp_id=b.liveramp_id where ttd_uid!='' and ttd_uid is not null and ttd_uid not like 'null' and a.liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug') group by dev_id" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_ttd_dev_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_comb_dev row format delimited fields terminated by '\t' as select dev_id,sum(count) as count from (select * from arr_577_rf_dev UNION ALL select * from arr_577_mm_dev UNION ALL select * from arr_577_ttd_dev) combi group by dev_id" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_comb_dev_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_rf_dev_match row format delimited fields terminated by '\t' as select mob_pi,count(*) as count from raw_lrapp_rf_407_tst group by mob_pi" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_rf_dev_match_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_mm_dev_match row format delimited fields terminated by '\t' as select mob_pi,count(*) as count from raw_lrapp_mm_407_tst group by mob_pi" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_mm_dev_match_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_ttd_dev_match row format delimited fields terminated by '\t' as select mob_pi,count(*) as count from raw_lrapp_ttd_407_tst group by mob_pi" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_ttd_dev_match_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;create table arr_577_comb_dev_match row format delimited fields terminated by '\t' as select mob_pi,sum(count) as count from (select * from arr_577_rf_dev_match UNION ALL select * from arr_577_mm_dev_match UNION ALL select * from arr_577_ttd_dev_match) combi group by mob_pi" -hiveconf mapreduce.job.queuename=AdhocMl > /tmp/ARR-577_comb_dev_match_tab.tsv &

~~~~~~~~~~~~~~~~~~~~~~~~~~

Accuracy

#####Tapad#####
nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table tapad_dsp_407_tab;create table tapad_dsp_407_tab row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.TTD_uid, a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, b.rf,b.ttd, size(b.rf) as rf_size, size(b.ttd) as rf_ttd from raw_cduid_407_data a join tapad_graph_final b on a.pubmatic_uid=b.pubmatic_uid where a.pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and a.pubmatic_uid is not null;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table tapad_dsp_407_rf_acc_tab; create table tapad_dsp_407_rf_acc_tab row format delimited fields terminated by '\t' as select \`date\`,pubmatic_uid, if(array_contains(rf,RF_15546_uid) or array_contains(rf,RF_22947_uid),1,0) as rf_acc from tapad_dsp_407_tab where pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and pubmatic_uid is not null and ((RF_22947_uid!='' and RF_22947_uid!='null' and RF_22947_uid is not null) or (RF_15546_uid!='' and RF_15546_uid!='null' and RF_15546_uid is not null)) and rf_size>0;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_rf_acc_tab_1.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select sum(rf_acc) as match,count(*) as total from tapad_dsp_407_rf_acc_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_rf_acc_data.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; drop table tapad_dsp_407_ttd_acc_tab;create table tapad_dsp_407_ttd_acc_tab row format delimited fields terminated by '\t' as select \`date\`,pubmatic_uid, if(array_contains(ttd,TTD_uid),1,0) as ttd_acc from tapad_dsp_407_tab where pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and pubmatic_uid is not null and TTD_uid!='' and TTD_uid!='null' and TTD_uid is not null and rf_ttd>0;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_ttd_acc_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select sum(ttd_acc) as match,count(*) as total from tapad_dsp_407_ttd_acc_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/tapad_dsp_407_ttd_acc_data.tsv &

#####Liveramp#####


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_ttd_407_tst;create table raw_lrapp_ttd_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.ttd_uid as b_ttd_uid  from raw_lrapp_407_orc a join liveramp_ttduid_407_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.ttd_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_ttd_tst_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; drop table lr_dsp_407_ttd_acc_tab;create table lr_dsp_407_ttd_acc_tab row format delimited fields terminated by '\t' as select \`date\`,pubmatic_uid, if(array_contains(b_ttd_uid,TTD_uid),1,0) as ttd_acc from raw_lrapp_ttd_407_tst where pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and pubmatic_uid is not null and TTD_uid!='' and TTD_uid!='null' and TTD_uid is not null;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_ttd_acc_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select sum(ttd_acc) as match,count(*) as total from lr_dsp_407_ttd_acc_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_ttd_acc_data.tsv &


~~~~~~~~~~~~~

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;drop table raw_lrapp_rf_407_tst;create table raw_lrapp_rf_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.rf_uid  from raw_lrapp_407_orc a join liveramp_rfuid_407_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.rf_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_rf_tst_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table lr_dsp_407_rf_acc_tab; create table lr_dsp_407_rf_acc_tab row format delimited fields terminated by '\t' as select \`date\`,pubmatic_uid, if(array_contains(rf_uid,RF_15546_uid) or array_contains(rf_uid,RF_22947_uid),1,0) as rf_acc from raw_lrapp_rf_407_tst where pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and pubmatic_uid is not null and ((RF_22947_uid!='' and RF_22947_uid!='null' and RF_22947_uid is not null) or (RF_15546_uid!='' and RF_15546_uid!='null' and RF_15546_uid is not null));" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_rf_acc_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select sum(rf_acc) as match,count(*) as total from lr_dsp_407_rf_acc_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_rf_acc_data.tsv &


~~~~~~~~~~~~

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=100;SET hive.exec.max.created.files=250000;create table raw_lrapp_mm_407_tst row format delimited fields terminated by '\t' as select a.\`date\`, a.hour, a.imprid, a.pubmatic_uid, a.it, a.browser, a.wops, a.dc, a.svr, a.RF_15546_uid, a.RF_22947_uid, a.MM_16736_uid, a.MM_16735_uid, a.TTD_uid, a.LR_uid, a.Neustar_uid ,a.mob_id, a.mob_pi, a.mob_ut, a.mob_uh, a.b_liveramp_id, b.mm_uid  from raw_lrapp_407_orc a join liveramp_mmuid_407_compressed_orc_snappy_dedup b on a.b_liveramp_id=b.liveramp_id where size(b.mm_uid)>0 and b_liveramp_id not in ('XY1193J6L8vQQA1wA0N1yEgn-DOrz2uNcxRyM9LmbhEmR9DmY','XY1193n5OYBS0cDoBJK7gxAhe-wy303RF2f2kdtrLYuOWn6_4','0','XY1193cINOSGM1IkUfgOq-SUaj4pogRYZug46-js2lcvPIzl8','XY1193nNF4tig_M8b_VgLeFVQp4L_nL7czU-yxrPs8EyA-o30','XY11938N4c7HIelOnkV0F9aqRv0Wsxsv4q4RXaJLkoHLZoaug');" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/raw_lrapp_407_mm_tst_tab.tsv &


nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000;drop table lr_dsp_407_mm_acc_tab; create table lr_dsp_407_mm_acc_tab row format delimited fields terminated by '\t' as select \`date\`,pubmatic_uid, if(array_contains(mm_uid,MM_16735_uid) or array_contains(mm_uid,MM_16736_uid),1,0) as mm_acc from raw_lrapp_mm_407_tst where pubmatic_uid not in ('','TCNE','tcne','NOUSERGUIDSPECIFIED') and pubmatic_uid is not null and ((MM_16736_uid!='' and MM_16736_uid!='null' and MM_16736_uid is not null) or (MM_16735_uid!='' and MM_16735_uid!='null' and MM_16735_uid is not null));" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_mm_acc_tab.tsv &

nohup hive -e"set hive.cli.print.header=true; set hive.exec.reducers.max=50;SET hive.exec.max.created.files=250000; select sum(mm_acc) as match,count(*) as total from lr_dsp_407_mm_acc_tab;" -hiveconf mapreduce.job.queuename=AdhocMl > /data1/MLAA/lr_dsp_407_mm_acc_data.tsv &





MM