-- you can concat multiple values :
select concat('Hey ', 'Mr','-',' Tambourine','-','Man')


-- Minus without minus as there is no minus in Athena:

select t.a,t.b,t.c from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)
 left  join
 (values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (14, 5, 6),
 (15, 6, 7),
 (16, 7, 8),
 (16, 8, 9),
 (16, 9, 10),
 (19, 10, 11)) as t2 (a,b,c)
 on t2.a = t.a and t.b=t2.b and t.c = t2.c
 where t2.a is null and t2.b is null and t2.c is null
 order by t.a;
 
 
 --CTAS 
 create table old_table 
 as select * from
 (values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)
 union 
 select * from 
 (values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (14, 5, 6),
 (15, 6, 7),
 (16, 7, 8),
 (16, 8, 9),
 (16, 9, 10),
 (19, 10, 11)) as t2 (a,b,c);
 
 
 
CREATE TABLE new_table
WITH (
	external_location = 's3://g-test1/my_stas_table/',
      format = 'Parquet',
      parquet_compression = 'SNAPPY'
)
AS SELECT *
FROM old_table;


-- Multiple with 
with 
a as (select count(*) as cnt from new_table),
b as (select cnt+10 as cnt_2 from a)
select cnt_2+100 from b

select reduce(array[1,2,3],0,(s, x) -> s + x, s -> s)
  
with
 t1 as (select * from 
 (values (0, 1, 2,10),
 (1, 2, 3,5),
 (1, 3, 4,3),
 (1, 4, 5,4),
 (14, 5, 6,7),
 (15, 6, 7,93),
 (16, 7, 8,12),
 (16, 8, 9,34),
 (16, 9, 10,12),
 (19, 10, 11,98)) as t1 (a,b,c,d)),
 tr as (select multimap_agg(t3.a,t1.d) as mv from 
 (values (1,3,5),(16,7,20)) as t3  (a,b,c),t1
 where t1.a=t3.a
 and t1.b>=t3.b 
 and t1.c<=t3.c
 group by t3.a)
 select map_keys(mv),reduce(map_values(mv)[1],0,(s, x) -> s + x, s -> s)
 from tr

select map_agg(a,c) from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)
 group by a
  
			    
select multimap_agg(a,c) from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)
 group by a
  
			    
select a,array_agg(b) from 
(values (0, '{1, 2}'),
 (1, '{2, 3}'),
 (1, '{3, 4}'),
 (1, '{4, 5}'),
 (4, '{5, 6}'),
 (5, '{6, 7}'),
 (6, '{7, 8}'),
 (6, '{8, 9}'),
 (6, '{9, 10}'),
 (9, '{10, 11}')) as t (a,b)
 group by a
 

  select max(c) from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)

  select max(c,2) from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 6),
 (5, 6, 7),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)

  
  
  
  select max_by(a,c) from 
(values (0, 1, 2),
 (1, 2, 3),
 (1, 3, 4),
 (1, 4, 5),
 (4, 5, 100),
 (5, 6, 102),
 (6, 7, 8),
 (6, 8, 9),
 (6, 9, 10),
 (9, 10, 11)) as t (a,b,c)
 
 
 WITH dataset AS (
 SELECT
 ARRAY['The CEO', 'is','Happy', 'because', 'customers','are','using','Our', 'Software'] as sentiment
)
SELECT actions FROM dataset
CROSS JOIN UNNEST(sentiment) as t(actions)


  SELECT count(*) FROM 
  (values (1,'sleep good'),
   	(2,'wake'),
   	(3,'regular'),
   	(4,'express'),
   (5,'lala'))
  lineitem (no,l_comment)
	 WHERE regexp_like(l_comment, 'wake|regular|express|sleep|hello')
	 
-- calculate number of hours in weekend and night shift
select name,sum(night_shift),sum(Weekend_hours)
from
(
select name,start_sh,end_sh
,cardinality(filter(sequence(start_sh,end_sh,INTERVAL '1' hour),x->date_format(x,'%H')>'18' or date_format(x,'%H')<'05')) as night_shift,
cardinality(filter(sequence(start_sh,end_sh,INTERVAL '1' hour),x->date_format(x,'%W')='Saturday' or date_format(x,'%W')='Sunday')) as Weekend_hours
from
(values 
('Mosh',date_parse('2019-12-15 10:00:12','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-15 23:00:12','%Y-%m-%d %H:%i:%s')),
('Mosh',date_parse('2019-08-05 15:02:47','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-06 19:22:53','%Y-%m-%d %H:%i:%s')),
('Idan',date_parse('2019-08-05 15:02:47','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-06 19:22:53','%Y-%m-%d %H:%i:%s')),
('Oshik',date_parse('2019-06-16 07:48:20','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-16 13:30:42','%Y-%m-%d %H:%i:%s')),
('Shlomy',date_parse('2019-05-11 04:29:38','%Y-%m-%d %H:%i:%s'),date_parse('2019-05-11 12:40:54','%Y-%m-%d %H:%i:%s')),
('Shlomy',date_parse('2019-11-15 14:53:21','%Y-%m-%d %H:%i:%s'),date_parse('2019-11-15 17:05:50','%Y-%m-%d %H:%i:%s')),
('Hanna',date_parse('2019-11-15 14:53:21','%Y-%m-%d %H:%i:%s'),date_parse('2019-11-15 17:05:50','%Y-%m-%d %H:%i:%s')),
('Jhonathan',date_parse('2019-08-22 05:23:10','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-23 10:48:51','%Y-%m-%d %H:%i:%s')),
('Menachem',date_parse('2019-09-30 03:23:09','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-30 09:44:03','%Y-%m-%d %H:%i:%s')),
('Yossi',date_parse('2019-02-07 02:06:30','%Y-%m-%d %H:%i:%s'),date_parse('2019-02-07 17:43:02','%Y-%m-%d %H:%i:%s')),
('Yaniv',date_parse('2019-07-28 08:44:03','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-29 12:43:28','%Y-%m-%d %H:%i:%s')),
('Jacky',date_parse('2019-03-14 06:39:50','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-14 19:32:10','%Y-%m-%d %H:%i:%s')),
('Reuven',date_parse('2019-08-16 01:32:17','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-17 05:13:44','%Y-%m-%d %H:%i:%s')),
('Baruch',date_parse('2019-01-25 18:21:22','%Y-%m-%d %H:%i:%s'),date_parse('2019-01-26 22:43:32','%Y-%m-%d %H:%i:%s')),
('Helena',date_parse('2019-12-17 07:27:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-18 15:29:19','%Y-%m-%d %H:%i:%s')),
('Michcky',date_parse('2019-10-11 16:39:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-12 21:43:17','%Y-%m-%d %H:%i:%s')),
('Tova',date_parse('2019-10-26 05:20:49','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-26 12:45:58','%Y-%m-%d %H:%i:%s')),
('Sarha',date_parse('2019-12-23 02:10:04','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-24 15:05:19','%Y-%m-%d %H:%i:%s')),
('Yochy',date_parse('2019-09-01 05:40:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-01 10:42:37','%Y-%m-%d %H:%i:%s')),
('Elcha',date_parse('2019-04-23 13:39:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-04-23 19:45:29','%Y-%m-%d %H:%i:%s')),
('Rivi',date_parse('2019-06-24 06:57:06','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-24 12:17:43','%Y-%m-%d %H:%i:%s')),
('Bili',date_parse('2019-07-09 10:46:56','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-10 17:36:36','%Y-%m-%d %H:%i:%s')),
('Eli',date_parse('2019-03-10 14:26:54','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-10 17:36:13','%Y-%m-%d %H:%i:%s')),
('Gil',date_parse('2019-07-02 11:26:39','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-03 22:36:55','%Y-%m-%d %H:%i:%s'))) 
t (name,start_sh,end_sh) 
)
group by name 
									
									
SELECT array_max(array[1,2,3]) AS max_element
		       
select flatten(array [array[1,2,3,4],array[5,6,7]])
					   
-- Time travel - we want to find the non special previues time wich is one month before 				   
with 
my_times as (select name,array_sort(filter(map_values(multimap_agg(name,if(special!='Y',start_sh)))[1],x->x is not null)) as a_times
from
(values 
('Mosh',date_parse('2019-12-15 10:00:12','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-15 23:00:12','%Y-%m-%d %H:%i:%s'),'N'),
('Mosh',date_parse('2019-08-05 15:02:47','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-06 19:22:53','%Y-%m-%d %H:%i:%s'),'N'),
('Mosh',date_parse('2019-06-16 07:48:20','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-16 13:30:42','%Y-%m-%d %H:%i:%s'),'Y'),
('Mosh',date_parse('2019-05-11 04:29:38','%Y-%m-%d %H:%i:%s'),date_parse('2019-05-11 12:40:54','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-11-15 14:53:21','%Y-%m-%d %H:%i:%s'),date_parse('2019-11-15 17:05:50','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-08-22 05:23:10','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-23 10:48:51','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-09-30 03:23:09','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-30 09:44:03','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-02-07 02:06:30','%Y-%m-%d %H:%i:%s'),date_parse('2019-02-07 17:43:02','%Y-%m-%d %H:%i:%s'),'Y'),
('Yaniv',date_parse('2019-07-28 08:44:03','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-29 12:43:28','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-03-14 06:39:50','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-14 19:32:10','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-08-16 01:32:17','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-17 05:13:44','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-01-25 18:21:22','%Y-%m-%d %H:%i:%s'),date_parse('2019-01-26 22:43:32','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-12-17 07:27:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-18 15:29:19','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-10-11 16:39:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-12 21:43:17','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-10-26 05:20:49','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-26 12:45:58','%Y-%m-%d %H:%i:%s'),'Y'),
('Illya',date_parse('2019-12-23 02:10:04','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-24 15:05:19','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-09-01 05:40:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-01 10:42:37','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-04-23 13:39:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-04-23 19:45:29','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-06-24 06:57:06','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-24 12:17:43','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-07-09 10:46:56','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-10 17:36:36','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-03-10 14:26:54','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-10 17:36:13','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-07-02 11:26:39','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-03 22:36:55','%Y-%m-%d %H:%i:%s'),'N')) 
t (name,start_sh,end_sh,special)
group by name)
select t.name,start_sh,end_sh,special,element_at(filter(a_times,x->x<date_add('month',-1,end_sh)),-1) as month_b
from 
(values 
('Mosh',date_parse('2019-12-15 10:00:12','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-15 23:00:12','%Y-%m-%d %H:%i:%s'),'N'),
('Mosh',date_parse('2019-08-05 15:02:47','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-06 19:22:53','%Y-%m-%d %H:%i:%s'),'N'),
('Mosh',date_parse('2019-06-16 07:48:20','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-16 13:30:42','%Y-%m-%d %H:%i:%s'),'Y'),
('Mosh',date_parse('2019-05-11 04:29:38','%Y-%m-%d %H:%i:%s'),date_parse('2019-05-11 12:40:54','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-11-15 14:53:21','%Y-%m-%d %H:%i:%s'),date_parse('2019-11-15 17:05:50','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-08-22 05:23:10','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-23 10:48:51','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-09-30 03:23:09','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-30 09:44:03','%Y-%m-%d %H:%i:%s'),'N'),
('Hanna',date_parse('2019-02-07 02:06:30','%Y-%m-%d %H:%i:%s'),date_parse('2019-02-07 17:43:02','%Y-%m-%d %H:%i:%s'),'Y'),
('Yaniv',date_parse('2019-07-28 08:44:03','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-29 12:43:28','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-03-14 06:39:50','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-14 19:32:10','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-08-16 01:32:17','%Y-%m-%d %H:%i:%s'),date_parse('2019-08-17 05:13:44','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-01-25 18:21:22','%Y-%m-%d %H:%i:%s'),date_parse('2019-01-26 22:43:32','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-12-17 07:27:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-18 15:29:19','%Y-%m-%d %H:%i:%s'),'N'),
('Yaniv',date_parse('2019-10-11 16:39:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-12 21:43:17','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-10-26 05:20:49','%Y-%m-%d %H:%i:%s'),date_parse('2019-10-26 12:45:58','%Y-%m-%d %H:%i:%s'),'Y'),
('Illya',date_parse('2019-12-23 02:10:04','%Y-%m-%d %H:%i:%s'),date_parse('2019-12-24 15:05:19','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-09-01 05:40:37','%Y-%m-%d %H:%i:%s'),date_parse('2019-09-01 10:42:37','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-04-23 13:39:46','%Y-%m-%d %H:%i:%s'),date_parse('2019-04-23 19:45:29','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-06-24 06:57:06','%Y-%m-%d %H:%i:%s'),date_parse('2019-06-24 12:17:43','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-07-09 10:46:56','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-10 17:36:36','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-03-10 14:26:54','%Y-%m-%d %H:%i:%s'),date_parse('2019-03-10 17:36:13','%Y-%m-%d %H:%i:%s'),'N'),
('Illya',date_parse('2019-07-02 11:26:39','%Y-%m-%d %H:%i:%s'),date_parse('2019-07-03 22:36:55','%Y-%m-%d %H:%i:%s'),'N')) 
t (name,start_sh,end_sh,special) 
left join my_times mt
on t.name = mt.name
order by name,start_sh
					   
					   
					   
					   
-- GeoSpacial example
-- based on and need pre-requesitis from : https://docs.aws.amazon.com/athena/latest/ug/geospatial-example-queries.html

SELECT counties.name,
        COUNT(*) cnt
FROM counties
CROSS JOIN earthquakes
WHERE ST_CONTAINS (ST_GeomFromLegacyBinary(counties.boundaryshape), ST_POINT(earthquakes.longitude, earthquakes.latitude))
GROUP BY  counties.name
ORDER BY  cnt DESC
