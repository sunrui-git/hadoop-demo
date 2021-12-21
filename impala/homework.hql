-- 创建表
drop table if exists user_clicklog;
create table user_clicklog (
        user_id string,
        click_time string
        )
row format delimited 
fields terminated by ",";

--加载数据
load data local inpath '/root/impala_data/clicklog.dat' into table user_clicklog;

-- 查看数据
select * from user_clicklog;

-- 1. 展示基础数据
select user_id, click_time, unix_timestamp(click_time) `timestamp`
from user_clicklog;

-- 2. 查出同一个用户时间间隔大于30分钟的记录
select user_id, click_time, `timestamp`,
    lag(`timestamp`) over(partition by user_id order by `timestamp`) prev,
    case when `timestamp` - (lag(`timestamp`) over(partition by user_id order by `timestamp`)) >= 1800 then 1 else 0 end isNew
from (select user_id, click_time, unix_timestamp(click_time) `timestamp`
        from user_clicklog ) t1;

-- 3. 累加isnew作为后面分组排序的依据
with tmp as (
    select user_id, click_time, `timestamp`,
        lag(`timestamp`) over(partition by user_id order by `timestamp`) prev,
        case when `timestamp` - (lag(`timestamp`) over(partition by user_id order by `timestamp`)) >= 1800 then 1 else 0 end isNew
    from (select user_id, click_time, unix_timestamp(click_time) `timestamp`
            from user_clicklog ) t1
) select user_id, click_time, `timestamp`,
    sum(isNew) over(partition by user_id order by `timestamp`) groupid
from tmp;

-- 4. 以user_id,groupid为分组依据，按时间顺序添加序号
with tmp as (
    select user_id, click_time, `timestamp`,
        lag(`timestamp`) over(partition by user_id order by `timestamp`) prev,
        case when `timestamp` - (lag(`timestamp`) over(partition by user_id order by `timestamp`)) >= 1800 then 1 else 0 end isNew
    from (select user_id, click_time, unix_timestamp(click_time) `timestamp`
            from user_clicklog ) t1
) 
select user_id, click_time, 
    row_number() over(partition by user_id, groupid order by `timestamp`) `index`
from (select user_id, click_time, `timestamp`,
        sum(isNew) over(partition by user_id order by `timestamp`) groupid
    from tmp) t2;