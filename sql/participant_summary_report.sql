select * from (
select * from (
select participant_id, global_participant_id, ssn, fund_balance_units*cast(price as decimal(22,7)) as fund_balance, civilian_group from datamart.participant_balance_summary where civilian_group is not null) PIVOT (SUM(fund_balance) FOR civilian_group in ('Employee','Automatic','Match'))
union 
select * from (
select participant_id, global_participant_id, ssn, fund_balance_units*cast(price as decimal(22,7)) as fund_balance, uniformed_group from datamart.participant_balance_summary where civilian_group is not null) PIVOT (SUM(fund_balance) FOR uniformed_group in ('Employee','Automatic','Match')))