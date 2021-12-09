
-- 1. 找出全部夺得3连贯的队伍
create table t1(
team string,
`year` int
)row format delimited 
fields terminated by ',';

load data local inpath "/root/data/t1.dat" into table t1;

select 
    team,
    year,
    rank() over(partition by team order by year asc) `rank`
from t1;

with tmp as (
select 
    team,
    year,
    rank() over(partition by team order by year asc) `rank`
from t1
) select team, `year`,`year` - `rank` gap from tmp;

with tmp as (
select 
    team,
    year,
    rank() over(partition by team order by year asc) `rank`
from t1
) 
select team, count(gap) `count` 
from (
    select  team, `year`, `year` - `rank` gap from tmp
) tmp2 
group by team, gap
having `count` >= 3;

-- 2. 找出每个id在在一天之内所有的波峰与波谷值
create table t2(
id string,
time string,
price double
)row format delimited fields terminated by ',';

load data local inpath "/root/data/t2.dat" into table t2;

select 
    id, 
    `time`, 
    price,
    lag(price) over(partition by id order by `time`) prev,
    lead(price) over(partition by id order by `time`) next
from t2;

with tmp as 
(
select 
    id, 
    `time`, 
    price,
    lag(price) over(partition by id order by `time`) prev,
    lead(price) over(partition by id order by `time`) next
from t2
) select id, `time`, price, prev, next,
if(price > prev and price > next, 1, 0) `波峰`,
if(price < prev and price < next, 1, 0) `波谷`
from tmp;

with tmp as 
(
select 
    id, 
    `time`, 
    price,
    lag(price) over(partition by id order by `time`) prev,
    lead(price) over(partition by id order by `time`) next
from t2
)
select id, `time`, price, if(up==1, "波峰","波谷") feature
from
(select id, `time`, price, prev, next,
if(price > prev and price > next, 1, 0) `up`,
if(price < prev and price < next, 1, 0) `down`
from tmp) tmp2
where up=1 or down=1;

-- 3、写SQL
-- 3.1、每个id浏览时长、步长
create table t3(
    id string,
    dt string,
    browseid string
)row format delimited fields terminated by '\t';

load data local inpath "/root/data/t3.dat" into table t3;

select 
id, 
round((
    max(unix_timestamp(replace(dt, "/", "-")||":00"))-
    min(unix_timestamp(replace(dt, "/", "-")||":00")) 
)/60, 0) `时长min`,
count(id) `步长`
from t3
group by id;

-- 3.2、如果两次浏览之间的间隔超过30分钟，认为是两个不同的浏览时间；再求每个id浏览时长、步长
with tmp as (
select 
id, dt, unix_timestamp(replace(dt, "/", "-")||":00") `timestamp`
from t3
) select id, dt, `timestamp`, 
lag(`timestamp`) over(partition by id order by `timestamp`) prev,
if (`timestamp` - lag(`timestamp`) over(partition by id order by `timestamp`) > 1800, 1, 0) isNew
from tmp;

with tmp as (
select 
id, dt, unix_timestamp(replace(dt, "/", "-")||":00") `timestamp`
from t3
) 
select id, dt, `timestamp`, 
lag(`timestamp`) over(partition by id order by `timestamp`) prev,
if (`timestamp` - lag(`timestamp`) over(partition by id order by `timestamp`) > 1800, 1, 0) isNew,
sum(if (`timestamp` - lag(`timestamp`) > 1800, 1, 0)) over(partition by id order by `timestamp` rows between unbounded preceding and current row) realnew
from tmp;

with tmp as (
select 
id, dt, unix_timestamp(replace(dt, "/", "-")||":00") `timestamp`
from t3
) 
select  
id, realnew,
round((max(`timestamp`)-min(`timestamp`))/60, 0) `时长min`,
count(id) `步长`
from 
(
select id, dt, `timestamp`, 
lag(`timestamp`) over(partition by id order by `timestamp`) prev,
if (`timestamp` - lag(`timestamp`) over(partition by id order by `timestamp`) > 1800, 1, 0) isNew,
sum(if (`timestamp` - lag(`timestamp`) > 1800, 1, 0)) over(partition by id order by `timestamp` rows between unbounded preceding and current row) realnew
from tmp
) tmp2
group by id, realnew;




