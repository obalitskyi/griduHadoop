#!/bin/bash

host=${1}
db=${2}
user=${3}
table=${4}
hdb=${5}
hTable=${6}

sqoop export --connect jdbc:mysql://${host}:3306/${db} --username ${user} --table ${table} --hcatalog-database ${hdb} --hcatalog-table ${hTable}