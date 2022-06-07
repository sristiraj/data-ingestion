select cast(participant_id as varchar(100)) as participant_id,
cast(global_participant_id as varchar(100)) as global_participant_id,
record_start_date,
record_end_date,
current_indicator,
hash_key,
create_date,
update_date,
update_userid,
max(cast(participant_firstname as varchar(100))) as participant_firstname,
max(cast(participant_lastname as varchar(100))) as participant_lastname,
max(cast(participant_middlename as varchar(100))) as participant_middlename,
max(cast(maritalStatus as varchar(100))) as maritalStatus,
max(cast(gender as varchar(100))) as gender,
max(cast(ssn as varchar(100))) as ssn,
max(cast(birth_date as varchar(100))) as birth_date,
age,
max(cast(department_code as varchar(10))) department_code,
max(cast(employer_agency_code as varchar(10))) employer_agency_code,
max(cast(payroll_office_number as varchar(100))) payroll_office_number,
max(cast(civilian_scd_date as varchar(100))) civilian_scd_date,
max(cast(uniformed_scd_date as varchar(100))) uniformed_scd_date,
max(cast(personnel_office_indicator as varchar(10))) personnel_office_indicator,
max(cast(employment_status as varchar(100))) employment_status,
max(cast(employment_status_description as varchar(100))) employment_status_description,
max(cast(employment_status_effective_date as varchar(100))) employment_status_effective_date,
max(cast(retirement_code_civil as varchar(100))) retirement_code_civil,
max(cast(retirement_catcode_civil as varchar(100))) retirement_catcode_civil,
max(cast(retirement_code_uniformed as varchar(100))) retirement_code_uniformed,
max(cast(retirement_catcode_uniformed as varchar(100))) retirement_catcode_uniformed,
max(cast(retirementDate as varchar(100))) retirementDate,
max(cast(hireDate as varchar(100))) hireDate,
max(cast(isTerminated as varchar(10))) isTerminated,
max(cast(terminationDate as varchar(100))) terminationDate,
max(cast(communication_preference as varchar(1000))) communication_preference,
max(cast(auto_enrollment_code as varchar(50))) auto_enrollment_code,
max(cast(auto_enrollment_date as varchar(100))) auto_enrollment_date,
max(cast(employment_code as varchar(100))) employment_code,
max(cast(employment_cd_date as varchar(100))) employment_cd_date,
max(cast(sourcesystemextractiontimestamp as TIMESTAMP)) as sourcesystemextractiontimestamp,
max(cast(messageTimestamp as TIMESTAMP)) as messageTimestamp,
max(cast(originatingpersoninternalid as VARCHAR(100))) originatingpersoninternalid
from (
with rt_header as (select * from (select *, row_number() over(partition by tbamessage_header_platforminternalid 
order by to_timestamp(case when tbamessage_header_sourcesystemextractiontimestamp="" 
then null else tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn 
from prod_alight_trusted_tba.tba_datachangeevents_root where partition_load_dt_tmstmp = (
select max(partition_load_dt_tmstmp) from prod_alight_trusted_tba.tba_datachangeevents_root )) x where rn=1),
udpdce as (select * from (select idm.message_body_create_idmapping_val_platforminternalid ,
udp.message_header_action,
udp.message_body_create_globalpersonidentifier ,
udp.message_header_messagetimestamp,
row_number() over(partition by idm.message_body_create_idmapping_val_platforminternalid order by 
to_timestamp(case when udp.message_header_messagetimestamp="" then null else udp.message_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn
from 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root_message_body_create_idmapping) idm 
left outer join 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root ) udp 
on udp.message_body_create_idmapping = idm.id and udp.partition_load_dt_tmstmp = idm.partition_load_dt_tmstmp
)
where rn=1)
select distinct rt.tbamessage_header_platforminternalid as participant_id,
udp.message_body_create_globalpersonidentifier as global_participant_id,
current_date as record_start_date,
to_date('9999-09-09','yyyy-MM-dd') as record_end_date,
cast(1 as long) as current_indicator,
sha2(concat(udp.message_body_create_globalpersonidentifier,concat(rt.tbamessage_header_platforminternalid,rt.tbamessage_header_nationaltaxid)),256) as hash_key,
current_date create_date,
current_date as update_date,
'glue' as update_userid,
rt.tbamessage_person_lastname as participant_lastname,
rt.tbamessage_person_firstname as participant_firstname,
rt.tbamessage_person_middlename as participant_middlename,
rt.tbamessage_person_gender as gender,
rt.tbamessage_person_maritalstatus as maritalStatus,
rt.tbamessage_header_nationaltaxid as ssn,
rt.tbamessage_person_birthdate as birth_date,
cast(round(months_between(current_date(), to_date(rt.tbamessage_person_birthdate,'yyyy-MM-dd'))/12,0) as long) as age,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
When '8337' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value 
When '8347' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END department_code,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid
WHEN '8357' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value
WHEN '8367' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END employer_agency_code,
case tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname
WHEN 'C-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue
WHEN 'U-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue 
ELSE null end payroll_office_number,
CASE tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname 
WHEN 'C-TSP-SCD-DATE' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue end civilian_scd_date,
CASE tbpavass.tbamessage_personattributes_val_additionalattributes_val_fieldname 
WHEN 'U-TSP-SCD-DATE' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue end uniformed_scd_date,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
WHEN '8377' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitcurrentorganizationcodes_val_value
WHEN '8387' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitcurrentorganizationcodes_val_value ELSE null END personnel_office_indicator,
rt.tbamessage_worker_currentemploymentstatus_employmentstatuscode as employment_status,
rt.tbamessage_worker_currentemploymentstatus_employmentstautseffectivedate as employment_status_effective_date,
rt.tbamessage_worker_currentemploymentstatus_employmentstautsfulldescription as employment_status_description,
rt.tbamessage_worker_retirementdate as retirementDate,
rt.tbamessage_worker_hiredate as hireDate,
rt.tbamessage_worker_isterminated as isTerminated,
rt.tbamessage_worker_terminationdate as terminationDate,
tbmsgcommpref.tbamessage_preferences_val_communicationpreferences_val_benefitscommunicationpreference as communication_preference,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8397' THEN '8397' ELSE null end retirement_code_civil,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8397' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null end retirement_catcode_civil,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8407' THEN '8407' ELSE null end retirement_code_uniformed,
CASE tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid
WHEN '8407' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null END retirment_catcode_uniformed,
CASE tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8597' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_begindate 
WHEN '8607' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_begindate ELSE null END auto_enrollment_date,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '21' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value 
WHEN '22' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null END employment_code,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '21' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate 
WHEN '22' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate ELSE null END employment_cd_date,
to_timestamp(case when rt.tbamessage_header_sourcesystemextractiontimestamp="" then null else rt.tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") sourcesystemextractiontimestamp,
to_timestamp(case when udp.message_header_messagetimestamp="" then null else udp.message_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") messageTimestamp,
tbapbar.tbamessage_personbenefitaccountrelationships_val_originatingpersoninternalid originatingpersoninternalid
FROM rt_header rt left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes tbwa 
on rt.tbamessage_benefitsworkerattributes = tbwa.id and rt.partition_load_dt_tmstmp = tbwa.partition_load_dt_tmstmp left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes tbwabcoc 
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes = tbwabcoc.id and 
 tbwa.partition_load_dt_tmstmp = tbwabcoc.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes tbpa 
on rt.tbamessage_personattributes = tbpa.id and rt.partition_load_dt_tmstmp = tbpa.partition_load_dt_tmstmp left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes_val_additionalattributes tbpavaa 
on tbpa.tbamessage_personattributes_val_additionalattributes = tbpavaa.id
and tbpa.partition_load_dt_tmstmp = tbpavaa.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses  tbwabcs 
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses = tbwabcs.id 
and tbwa.partition_load_dt_tmstmp = tbwabcs.partition_load_dt_tmstmp
left outer join 
udpdce udp on rt.tbamessage_header_platforminternalid = 
udp.message_body_create_idmapping_val_platforminternalid
left outer join  prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personbenefitsaccountrelationships tbapbar
on rt.tbamessage_personbenefitaccountrelationships = tbapbar.id
and rt.partition_load_dt_tmstmp = tbapbar.partition_load_dt_tmstmp
left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_preferences tbmsgpref 
on rt.tbamessage_preferences = tbmsgpref.id 
and rt.partition_load_dt_tmstmp = tbmsgpref.partition_load_dt_tmstmp
left outer join
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_preferences_val_communicationpreferences tbmsgcommpref on 
tbmsgpref.tbamessage_preferences_val_communicationpreferences = tbmsgcommpref.id 
and tbmsgpref.partition_load_dt_tmstmp = tbmsgcommpref.partition_load_dt_tmstmp
where 
rt.tbamessage_header_platforminternalid is not null and
rt.tbamessage_header_subtopic = 'PersonWorker' and udp.message_header_action = 'Create'

union

with rt_header1 as (select * from (select *, row_number() over(partition by tbamessage_header_platforminternalid 
order by to_timestamp(case when tbamessage_header_sourcesystemextractiontimestamp="" then null else tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn from prod_alight_trusted_tba.tba_datachangeevents_root where partition_load_dt_tmstmp = (select max(partition_load_dt_tmstmp) from prod_alight_trusted_tba.tba_datachangeevents_root )) x where rn=1),
udpce1 as (select * from (select idm.message_body_update_after_modificationhistory_val_sourcesysteminternalid ,
udp.message_header_action,
udp.message_body_update_after_globalpersonidentifier,
udp.message_header_messagetimestamp,
row_number() over(partition by idm.message_body_update_after_modificationhistory_val_sourcesysteminternalid order by 
to_timestamp(case when udp.message_header_messagetimestamp="" then null else udp.message_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn
from 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root_message_body_update_after_modificationhistory) idm 
left outer join 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root ) udp 
on udp.message_body_after_modificationhistory  = idm.id and udp.partition_load_dt_tmstmp = idm.partition_load_dt_tmstmp)
where rn=1)
select distinct rt.tbamessage_header_platforminternalid as participant_id,
udp.message_body_update_after_globalpersonidentifier as global_participant_id,
current_date as record_start_date,
to_date('9999-09-09','yyyy-MM-dd') as record_end_date,
cast(1 as long) as current_indicator,
sha2(concat(udp.message_body_update_after_globalpersonidentifier,concat(rt.tbamessage_header_platforminternalid,rt.tbamessage_header_nationaltaxid)),256) as hash_key,
current_date create_date,
current_date as update_date,
'glue' as update_userid,
rt.tbamessage_person_lastname as participant_lastname,
rt.tbamessage_person_firstname as participant_firstname,
rt.tbamessage_person_middlename as participant_middlename,
rt.tbamessage_person_gender as gender,
rt.tbamessage_person_maritalstatus as maritalStatus,
rt.tbamessage_header_nationaltaxid as ssn,
rt.tbamessage_person_birthdate as birth_date,
cast(round(months_between(current_date(), to_date(rt.tbamessage_person_birthdate,'yyyy-MM-dd'))/12,0) as long) as age,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
When '8337' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value 
When '8347' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END department_code,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid
WHEN '8357' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value
WHEN '8367' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_value ELSE null END employer_agency_code,
case tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname
WHEN 'C-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue
WHEN 'U-PAYROLL-OFF-NUM' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue 
ELSE null end payroll_office_number,
CASE tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname 
WHEN 'C-TSP-SCD-DATE' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue end civilian_scd_date,
CASE tbpavass.tbamessage_personattributes_val_additionalattributes_val_fieldname 
WHEN 'U-TSP-SCD-DATE' THEN tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldvalue end uniformed_scd_date,
case tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes_val_organizationid 
WHEN '8377' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitcurrentorganizationcodes_val_value
WHEN '8387' THEN tbwabcoc.tbamessage_benefitsworkerattributes_val_benefitcurrentorganizationcodes_val_value ELSE null END personnel_office_indicator,
rt.tbamessage_worker_currentemploymentstatus_employmentstatuscode as employment_status,
rt.tbamessage_worker_currentemploymentstatus_employmentstautseffectivedate as employment_status_effective_date,
rt.tbamessage_worker_currentemploymentstatus_employmentstautsfulldescription as employment_status_description,
rt.tbamessage_worker_retirementdate as retirementDate,
rt.tbamessage_worker_hiredate as hireDate,
rt.tbamessage_worker_isterminated as isTerminated,
rt.tbamessage_worker_terminationdate as terminationDate,
tbmsgcommpref.tbamessage_preferences_val_communicationpreferences_val_benefitscommunicationpreference as communication_preference,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8397' THEN '8397' ELSE null end retirement_code_civil,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8397' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null end retirement_catcode_civil,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8407' THEN '8407' ELSE null end retirement_code_uniformed,
CASE tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid
WHEN '8407' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null END retirment_catcode_uniformed,
CASE tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '8597' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_begindate 
WHEN '8607' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_begindate ELSE null END auto_enrollment_date,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '21' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value 
WHEN '22' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_value ELSE null END employment_code,
case tbwabcs.tbamessage_benefitsworkerattributes_val_benefitcurrentemploymentstatuses_val_categoryid 
WHEN '21' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate 
WHEN '22' THEN tbwabcs.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses_val_begindate ELSE null END employment_cd_date,
to_utc_timestamp(to_timestamp(case when rt.tbamessage_header_sourcesystemextractiontimestamp="" then null else rt.tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz"),"CDT") sourcesystemextractiontimestamp,
to_utc_timestamp(to_timestamp(case when udp.message_header_messagetimestamp="" then null else udp.message_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz"),"CDT") messageTimestamp,
tbapbar.tbamessage_personbenefitaccountrelationships_val_originatingpersoninternalid originatingpersoninternalid
FROM rt_header1 rt left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes tbwa 
on rt.tbamessage_benefitsworkerattributes = tbwa.id 
and rt.partition_load_dt_tmstmp = tbwa.partition_load_dt_tmstmp left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes tbwabcoc 
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentorganizationcodes = tbwabcoc.id 
and tbwa.partition_load_dt_tmstmp = tbwabococ.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes tbpa 
on rt.tbamessage_personattributes = tbpa.id 
and rt.partition_load_dt_tmstmp = tbpa.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes_val_additionalattributes tbpavaa 
on tbpa.tbamessage_personattributes_val_additionalattributes = tbpavaa.id
and tbpa.partition_load_dt_tmstmp = tbpavaa.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses  tbwabcs 
on tbwa.tbamessage_benefitsworkerattributes_val_benefitscurrentemploymentstatuses = tbwabcs.id 
and  tbwa.partition_load_dt_tmstmp =  tbwabcs.partition_load_dt_tmstmp
left outer join udpce1 udp 
on rt.tbamessage_header_platforminternalid = 
udp.message_body_update_after_modificationhistory_val_sourcesysteminternalid 
left outer join  prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personbenefitsaccountrelationships tbapbar
on rt.tbamessage_personbenefitsaccountrelationships = tbapbar.id
and rt.partition_load_dt_tmstmp = tbapbar.partition_load_dt_tmstmp
left outer join 
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_preferences tbmsgpref 
on rt.tbamessage_preferences = tbmsgpref.id 
and rt.partition_load_dt_tmstmp = tbmsgpref.partition_load_dt_tmstmp
left outer join
prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_preferences_val_communicationpreferences tbmsgcommpref on 
tbmsgpref.tbamessage_preferences_val_communicationpreferences = tbmsgcommpref.id 
and tbmsgpref.partition_load_dt_tmstmp = tbmsgcommpref.partition_load_dt_tmstmp
where 
rt.tbamessage_header_platforminternalid is not null and
rt.tbamessage_header_subtopic = 'PersonWorker' and udp.message_header_action = 'Update'
) tab group by participant_id,global_participant_id,record_start_date,record_end_date,current_indicator,hash_key,create_date,update_date,
update_userid,age

































 
