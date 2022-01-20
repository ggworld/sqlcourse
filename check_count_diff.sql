-- To find duplicate loads of data with some huristics of 30% and if the original is less than 10000 ignore 

with yaniv_t as (
select 'yaniv_t' as src,"year" ,"month" ,count(1) as cnt
from demand_forecast.sales_view yt
group by 1,2,3),
geva_t as (
select 'gevat_t' as src,"year" ,"month" ,count(1) as cnt
from g_dem_forcast.sales_view yt
group by 1,2,3
)
select geva_t.year,geva_t.month,geva_t.cnt as geva,yaniv_t.cnt as yaniv, geva_t.cnt/yaniv_t.cnt*1.0 as diff
from geva_t join yaniv_t on (geva_t.year=yaniv_t.year and geva_t.month=yaniv_t.month)
where geva_t.cnt!=yaniv_t.cnt and geva_t.cnt not between yaniv_t.cnt*0.7 and yaniv_t.cnt*1.3
and yaniv_t.cnt > 10000
order by geva_t.cnt-yaniv_t.cnt desc
