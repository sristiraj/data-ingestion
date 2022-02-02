select distinct unpiv.part_ssn, unpiv.plan_year, unpiv.activity, 
case when temp_mapping.Employee is not null AND  temp_mapping.Employee != '' then unpiv.value end Employee,
case when temp_mapping.Automatic is not null AND  temp_mapping.Automatic != '' then unpiv.value end Automatic, 
case when temp_mapping.Matching is not null AND  temp_mapping.Matching != '' then unpiv.value end Matching from (
select part_num as part_ssn, plan_year, stack(11,'401 EE contribution',cntrb_401k_ee,'415 Contribution',cntrb_415,'401K Catchup',cntrb_401k_ctchup,'Roth Contribution',
cntrb_roth, 'agency Match', agency_match, 'Agency Automatic 1%, 2 year vested', agency_auto_2yr, 'Employee Tax-Exempt', part_tax_exempt,  'Employee Tax-Defered', part_tax_defer, 'Part Tax Defferred Rollover', roll_in_tax_excempt , 'Roth Deferred', part_roth, 'Participant Roth Rollover', roll_in_roth) as (activity,value) from piv) unpiv join temp_mapping
on unpiv.activity = temp_mapping.Activity 
order by 1,2,3
