use obalitskyi;

drop table top10Categories;
create table top10Categories (
    category string,
    sum bigint
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile;

insert overwrite table top10Categories
    select category, count(*) as sum from events group by category order by sum desc limit 10;
