create database if not exists obalitskyi;

use obalitskyi;

drop table if exists top10Categories;
create table top10Categories (
                                 category varchar(255),
                                 sum bigint
);