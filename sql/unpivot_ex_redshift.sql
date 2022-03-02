SELECT * 
FROM (
    SELECT *
    FROM datamart.lkup_fund_description
) UNPIVOT INCLUDE NULLS (
    cnt FOR color IN (fund_pos_12, fund_long_name, fund_short_name)
);
