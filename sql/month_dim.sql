with series as (select explode(sequence(to_date('20000101','yyyyMMdd'),to_date(concat(cast(date_format(CURRENT_date,'yyyy')+5 as int),'1231'),'yyyyMMdd'),interval 1 day)) as dt)
select cast(row_number() over(order by month_code) as long) month_id,
month_description, end_of_month_date, month_code from 
(select distinct
month_description,
end_of_month_date,
month_code from (
select 
dt as date,
lower(date_format(dt,'MMMM')) as month_description,
last_day(dt) as end_of_month_date,
date_format(dt,'yyyyMM') month_code
from series)) order by month_code
