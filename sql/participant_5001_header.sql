select distinct rt.tbamessage_header_platforminternalid as participant_id,
rt.tbamessage_header_globalPersonIdentifier as global_participant_id,
current_date as record_start_date,
to_date('9999-09-09','yyyy-MM-dd') as record_end_date,
cast(1 as long) as current_indicator,
sha2(concat(rt.tbamessage_header_platforminternalid,rt.tbamessage_header_globalPersonIdentifier,rt.tbamessage_header_nationaltaxid,rt.tbamessage_person_birthdate,tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value,tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue,tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value),256) as Hashkey,
current_date create_date,
current_date as update_date,
'glue' as update_userid,
cast(null as string) as participant_name,
rt.tbamessage_header_nationaltaxid ssn,
rt.tbamessage_person_birthdate as birth_date,
cast(round(months_between(current_timestamp(), rt.tbamessage_person_birthdate)/12,2) as long) as age,
case   tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
WHEN 8337 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value
WHEN 8347 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END department_code,
case   tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
WHEN 8357 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value
WHEN 8367 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END employer_agency_code,
case tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname
WHEN 'C-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue
WHEN 'U-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue
ELSE null end payroll_office_number,
case   tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
WHEN 8377 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value
WHEN 8387 THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END personnel_office_indicator,
cast(null as string) as employment_status,
case   tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_categoryid
WHEN 21 THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value
WHEN 22 THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null END auto_enrollment_code,
case   tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_categoryid
WHEN 21 THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate
WHEN 22 THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate ELSE null END auto_enrollment__date
from sandbox_ss_tba.tba_root rt left outer join sandbox_ss_tba.tba_root_tbamessage_benefitsworkerattributes tbwa
on rt.tbamessage_benefitsworkerattributes=tbwa.id left outer join 
sandbox_ss_tba.tba_root_tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes tbwabcoc
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes=tbwabcoc.id
left outer join sandbox_ss_tba.tba_root_tbamessage_personattributes tbpa 
on rt.tbamessage_personattributes=tbpa.id left outer join 
sandbox_ss_tba.tba_root_tbamessage_personattributes_val_additionalattributes tbpavaa
on tbpa.tbamessage_personattributes_val_additionalattributes = tbpavaa.id
left outer join sandbox_ss_tba.tba_root_tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses tbwabcs
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses=tbwabcs.id
