with series as (select explode(sequence(to_date('20000101','yyyyMMdd'),to_date(concat(cast(date_format(CURRENT_date,'yyyy')+5 as int),'1231'),'yyyyMMdd'),interval 1 day)) as dt),
federal_holiday as (
select to_date('20000101','yyyyMMdd') holiday_dt, 'AL' as holiday_description
)
select date_format(dt,'yyyyMMdd') date_id,
dt as date,
lower(date_format(dt,'EEEE')) as date_of_week,
case when lower(date_format(dt,'EEEE')) = 'saturday' or  lower(date_format(dt,'EEEE')) = 'sunday' or  federal_holiday.holiday_dt is not null then 'Y'
else 'N' end is_business_day,
case when federal_holiday.holiday_dt is not null then 'Y'
else 'N' end is_federal_holiday,
federal_holiday.holiday_description
from series left outer join federal_holiday on series.dt=federal_holiday.holiday_dt
