select fd.participant_id, fd.global_participant_id, fd.ssn, fd.employee, fd.automatic, fd.match, fds.fund_description  from (
select * from (
select participant_id, global_participant_id, ssn, fund_balance_units*cast(price as decimal(38,2)) as fund_balance, civilian_group from datamart.participant_balance_summary where civilian_group is not null
and is_decimal(price) IS TRUE) 
PIVOT (SUM(fund_balance) FOR civilian_group in ('Employee','Automatic','Match'))
union 
select * from (
select participant_id, global_participant_id, ssn, fund_balance_units*cast(price as decimal(38,2)) as fund_balance, uniformed_group from datamart.participant_balance_summary where civilian_group is not null
and is_decimal(price) IS TRUE) 
PIVOT (SUM(fund_balance) FOR uniformed_group in ('Employee','Automatic','Match'))
) fd left outer join sandbox_ss_tba.lkup_tba_fund_description fds on
f.fund_id = fds.fund_id
