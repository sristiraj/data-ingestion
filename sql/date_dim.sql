with series as (select explode(sequence(to_date('20000101','yyyyMMdd'),to_date(concat(cast(date_format(CURRENT_date,'yyyy')+5 as int),'1231'),'yyyyMMdd'),interval 1 day)) as dt)
select date_format(dt,'yyyyMMdd') date_wid,
dt as date,
lower(date_format(dt,'EEEE')) as date_of_week,
cast(null as varchar(10)) is_business_day,
cast(null as varchar(10)) is_federal_holiday,
cast(null as varchar(100)) holiday_description
from series 
