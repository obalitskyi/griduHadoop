use obalitskyi;

drop table top10ProductsInCategories;
create table top10ProductsInCategories (
    product string,
    category string,
    num_of_transactions bigint
)
    row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    stored as textfile;

insert overwrite table top10ProductsInCategories
    select product, category, num_of_transactions from
        (select product, category, num_of_transactions, row_number()
                    over (partition by category order by num_of_transactions desc) as rank from
            (select product, category, count(*) as num_of_transactions from events group by product, category) ev) ranked
        where ranked.rank <= 10 order by category, num_of_transactions desc;
