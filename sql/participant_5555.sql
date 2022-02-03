select distinct cast(unpiv.part_ssn as long) as part_ssn, cast(unpiv.plan_year as long) plan_year, cast(unpiv.activity as string) as activity, 
cast(case when temp_mapping.Employee is not null AND  temp_mapping.Employee != '' then unpiv.value end as decimal(10,2)) employee_contribution,
cast(case when temp_mapping.Automatic is not null AND  temp_mapping.Automatic != '' then unpiv.value end as decimal(10,2)) agency_automatic, 
cast(case when temp_mapping.Matching is not null AND  temp_mapping.Matching != '' then unpiv.value end as decimal(10,2)) matching_dollar_contribution from (
select part_ssn, plan_year, stack(11,'401 EE contribution',cntrb_401k_ee,'415 Contribution',cntrb_415,'401K Catchup',cntrb_401k_ctchup,'Roth Contribution',
cntrb_roth, 'agency Match', agency_match, 'Agency Automatic 1%, 2 year vested', agency_auto_2yr, 'Employee Tax-Exempt', part_tax_exempt,  'Employee Tax-Defered', part_tax_defer, 'Part Tax Defferred Rollover', roll_in_tax_excempt , 'Roth Deferred', part_roth, 'Participant Roth Rollover', roll_in_roth) as (activity,value) from gluesession001db.participant_5555_src as piv) unpiv join gluesession001db.participant_5555_mapping temp_mapping
on unpiv.activity = temp_mapping.Activity 
order by 1,2,3
