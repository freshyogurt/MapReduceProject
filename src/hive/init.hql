create database if not exists stackoverflow;

use stackoverflow;

set hive.exec.mode.local.auto=true;

create table if not exists posts(
id int,
posttypeid int,
parentid int,
acceptedanswerid int,
creationdate timestamp)
row format delimited
fields terminated by ',';

load data
local inpath '/home/nan/cs6240/project/Posts2_hive.csv'
overwrite into table posts;
