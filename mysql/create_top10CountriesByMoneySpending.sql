create database if not exists obalitskyi;

use obalitskyi;

drop table if exists top10CountriesByMoneySpending;
create table top10CountriesByMoneySpending (
                                 country_name varchar(255),
                                 sum double
);