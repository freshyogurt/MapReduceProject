set mapred.reduce.tasks=10;

create database if not exists stackoverflow;

use stackoverflow;

create external table if not exists posts(
	id int,
	posttypeid int,
	parentid int,
	acceptedanswerid int,
	creationdate timestamp
)
row format delimited
fields terminated by ','
LOCATION 's3://cs62402014fall/input/Posts2.in';

create external table if not exists results(
	id int,
	creationdate timestamp,
	timegap int
)
row format delimited
fields terminated by ','
LOCATION 's3://cs62402014fall/output/stackoverflow/hive';

INSERT OVERWRITE TABLE results
select a.id,
	   a.creationdate,
	   MIN(UNIX_TIMESTAMP(b.creationdate) - UNIX_TIMESTAMP(a.creationdate))
from posts a
join posts b
on a.id = b.parentid
where UNIX_TIMESTAMP(a.creationdate) >= UNIX_TIMESTAMP('2009-01-01 00:00:00')
	  AND UNIX_TIMESTAMP(a.creationdate) < UNIX_TIMESTAMP('2014-01-01 00:00:00')
GROUP BY a.id,
		 a.creationdate;