create database if not exists obalitskyi;

use obalitskyi;

drop table if exists top10ProductsInCategories;
create table top10ProductsInCategories (
                                 product varchar(255),
                                 category varchar(255),
                                 num_of_transactions bigint
);