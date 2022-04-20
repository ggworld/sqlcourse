WITH dataset AS (
 SELECT
 ARRAY['Amir', 'is','Happy', 'from', 'Tyson','Using','aiOla'] as users
)
SELECT names FROM dataset
CROSS JOIN UNNEST(users) as t(names)


and with map
with get_inspections
as (select element_at(D,'inspect') as inspect from 
(values (1,2,3,map(array ['inspect'], array[ array[map (array ['user','result'], array ['1','20']),map (array ['user','result'], array ['2','30'])]])
)
) as t1  (A,B,C,D))
select names['user'] FROM get_inspections
CROSS JOIN UNNEST(inspect) as t(names)
