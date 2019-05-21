create database if not exists obalitskyi;

use obalitskyi;

create external table if not exists events (
    product string,
    price double,
    eventtime timestamp,
    category string,
    ip string
)
    partitioned by (date string)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    with serdeproperties ("timestamp.formats" = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    stored as textfile
    location '${hiveconf:location}';

alter table events add partition (date='2019-05-01') location '${hiveconf:location}/2019/05/01';
alter table events add partition (date='2019-05-02') location '${hiveconf:location}/2019/05/02';
alter table events add partition (date='2019-05-03') location '${hiveconf:location}/2019/05/03';
alter table events add partition (date='2019-05-04') location '${hiveconf:location}/2019/05/04';
alter table events add partition (date='2019-05-05') location '${hiveconf:location}/2019/05/05';
alter table events add partition (date='2019-05-06') location '${hiveconf:location}/2019/05/06';
alter table events add partition (date='2019-05-07') location '${hiveconf:location}/2019/05/07';
