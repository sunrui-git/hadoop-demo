#!bin/bash
# 统计出每日活跃用户数, 并插入user_info表中
dateStr=${date -d '1 days ago' +%Y%m%d}
echo "$dateStr"
hql="insert into table default.user_info
      select active_num, '$dateStr' dateStr
      from (
          select count(distinct id) active_num
          from default.user_clicks
          where dt='$dateStr'
      ) tmp"

hive -e "$hql"

