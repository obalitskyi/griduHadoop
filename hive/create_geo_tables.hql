create database if not exists obalitskyi;

use obalitskyi;

create external table if not exists ip (
    network string,
    geoname_id bigint,
    registered_country_geoname_id bigint,
    represented_country_geoname_id bigint,
    is_anonymous_proxy boolean,
    is_satellite_provider boolean
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile
    location '${hiveconf:ip_location}'
    tblproperties ("skip.header.line.count"="1");

create external table if not exists ipinfo (
    geoname_id bigint,
    locale_code string,
    continent_code string,
    continent_name string,
    country_iso_code string,
    country_name string,
    is_in_european_union boolean
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile
    location '${hiveconf:ipinfo_location}'
    tblproperties ("skip.header.line.count"="1");
