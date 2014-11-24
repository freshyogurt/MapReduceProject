use stackoverflow;

select a.id,(UNIX_TIMESTAMP(b.creationdate) - UNIX_TIMESTAMP(a.creationdate)) as timeGap, hour(a.creationdate) as creatHour
from posts a
left outer join posts b
on a.acceptedanswerid = b.id
where UNIX_TIMESTAMP(a.creationdate) >= UNIX_TIMESTAMP('2014-01-01 00:00:00');
