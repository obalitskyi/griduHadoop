use obalitskyi;

drop table top10CountriesByMoneySpending;
create table top10CountriesByMoneySpending (
    country_name string,
    sum bigint
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile;

set hive.auto.convert.join=true;

insert overwrite table top10CountriesByMoneySpending
    select country_name, sum(price) as sum from
         ip ipp join events e on ipp.network = e.ip
            join ipinfo inf on ipp.geoname_id = inf.geoname_id
        group by country_name
        having country_name is not NULL and country_name != ''
        order by sum desc limit 10;