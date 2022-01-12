
-- 原始数据分区表
drop table user_clicks;
create table user_clicks(
    id string,
    click_time string,
    index string) 
partitioned by(dt string) 
row format delimited 
fields terminated by ' ';

-- 需要开发一个import.job每日从hdfs对应日期目录下同步数据到该表指定分区
load data inpath '/user_clicks/20220103/' into table default.user_clicks partition(dt='20220103')

select * from user_clicks;

-- 指标表
drop table user_info;
create table user_info(
    active_num string,
    dateStr string) 
row format delimited 
fields terminated by '\t';

select * from user_info;

-- 需要开发一个analysis.job依赖import.job执行，统计出每日活跃用户(一个用户出现多次算作一次)数并插入user_info表中。
insert into table default.user_info
select active_num, "20220101" dateStr
from (
select count(distinct id) active_num
from default.user_clicks 
where dt="20220101"
) tmp;
