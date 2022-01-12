#!bin/bash
# 每天晚上2点，导入前一天的数据
dateStr=${date -d '1 days ago' +%Y%m%d}
echo "$dateStr"
hql="load data inpath '/user_clicks/${dateStr}/' into table default.user_clicks partition(dt='${dateStr}')"

echo "$hql"

hive -e "$hql"