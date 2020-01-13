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
