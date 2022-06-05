with rt_header as (select * from (select *, row_number() over(partition by tbamessage_header_platforminternalid 
order by to_timestamp(case when rt.tbamessage_header_sourcesystemextractiontimestamp="" then null else rt.tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn from prod_alight_trusted_tba.tba_datachangeevents_root ) x where rn=1),
udpdce as (select * from (select idm.message_body_create_idmapping_val_platforminternalid ,
udp.message_header_action,
udp.message_body_create_globalpersonidentifier ,
udp.tbamessage_header_messagetimestamp,
row_number() over(partition by idm.message_body_create_idmapping_val_platforminternalid order by 
to_timestamp(case when udp.tbamessage_header_messagetimestamp="" then null else udp.tbamessage_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn
from 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root_message_body_create_idmapping
where partition_load_dt_tmstmp=(select max(partition_load_dt_tmstmp) 
from prod_alight_trusted_tba.tba_udpdatachangeevents_root_message_body_create_idmapping)  ) idm 
left outer join 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root where partition_load_dt_tmstmp=(select max(partition_load_dt_tmstmp) from
 prod_alight_trusted_tba.tba_udpdatachangeevents_root)  ) udp 
on udp.message_body_create_idmapping = idm.id and udp.partition_load_dt_tmstmp = idm.partition_load_dt_tmstmp
)
where rn=1)
select distinct cast(rt.tbamessage_header_platforminternalid as varchar(1000)) as participant_id,
cast(udp.message_body_create_globalpersonidentifier as varchar(1000)) as global_participant_id,
current_date('9999-09-09','YYYY-MM-dd') as record_end_date,
cast(1 as long) as current_indicator,
current_date create_date,
current_date as update_date,
'glue' as update_userid,
cast(null as string) as participant_name,
cast(rt.tbamessage_header_nationaltaxid as varchar(1000)) ssn,
cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_fundid as varchar(100)) fundid,
cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_subfundi as varchar(100)) subfundid,
cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid as varchar(100)) accountid,
CAST(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_planaccountfundbalanceunits as DECIMAL(22,7)) fund_balance_units,
CAST(lfp.price AS DECIMAL(12,6)) as price,
CAST (CASE WHEN tbpavaaa.tbamessage_personattributes_val_addtionalattributes_val_fieldname is not null then ltam_civ.'group' else null end as varchar(100)) as civilian_group, 
cast(case when tbpavaa1.tbamessage_personattributes_val_additonalattributes_val_fieldname is not null then itam_civ.'group' else null end as varchar(100)) uniformed_group,
to_timestamp(case when rt.tbamessage_header_sourcesystemextractiontimestamp="" then null else rt.tbamessage_header_sourcesystemextractiontimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") sourcesystemextractiontimestamp,
to_timestamp(case when udpdce.tbamessage_header_messagetimestamp="" then null else udpdce.tbamessage_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") messageTimestamp,
cast(tbapbar.tbamessage_personbenefitaccountrelationships_val_originatingpersoninternalid as varchar(100)) originatingpersoninternalid
from
rt_header rt left outer join prod_alight_trusted_tba.tba_datachangeevents_root rt_acc
on rt.tbamessage_header_platforminternalid = rt_acc.tbamessage_header_platforminternalid 
and rt.partition_load_dt_tmstmp = rt_acc.partition_load_dt_tmstmp
and rt_acc.tbamessage_header_subtopic = 'DCUnitsRORContributions' 
and rt.tbamessage_header_subtopic = 'PersonWorker' 
left outer join udpdce udp
on rt.tbamessage_header_platforminternalid = udp.message_body_create_idmapping_val_platforminternalid 
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_definedcontributionbenefits_definedcontributionplans tbadcbdcp 
on rt_acc.tbamessage_definedcontributionbenefits_definedcontributionplans = tbadcbdcp.id 
and rt_acc.partition_load_dt_tmstmp = tbadcbdcp.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_roottbamessage_definedcontributionbebefits_definedcontributionplans_val_planaccountfundtable tbadcbdcppft 
on tbadcbdcp.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable = tbadcbdcppft.id  
and tbadcbdcp.partition_load_dt_tmstmp = tbadcbdcppft.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes tbpa  
on rt.tbamessage_personattributes = tbpa.id 
and rt.partition_load_dt_tmstmp = tbpa.partition_load_dt_tmstmp
left outer join prod_alight_trusted_tba.lkup_fund_price lfp 
on cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_fundid as varchar(10)) = 
cast(lfp.fund_id as varchar(10)) 
left outer join prod_alight_trusted_tba.lkup_accountid_group ltam_civ on cast(ltam_civ.tba_accountid_civilian as varchar(100)) = 
cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid as varchar(100)) 
left outer join prod_alight_trusted_tba.lkup_accountid_group ltam_uni on 
cast(ltam_uni.tba_accountid_uniform as varchar(100)) = 
cast(tbadcbdcppft.tbamessage_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid as varchar(100))
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes_val_additionalattributes tbpavaa 
on tbpa.tbamessage_personattributes_val_additonalattributes = tbpavaa.id and 
tbpa.partition_load_dt_tmstmp = tbpavaa.partition_load_dt_tmstmp and 
tbpavaa.tbamessage_personattributes_val_additonalattributes_val_fieldname = 'C-PAYROLL-OFF-NUM' 
left outer join prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personattributes_val_additionalattributes tbpavaal 
on tbpa.tbamessage_personattributes_val_additonalattributes tbpavaa1.id 
and tbpa.partition_load_dt_tmstmp = tbpavaa1.partition_load_dt_tmstmp
and tbpavaa1.tbameesage_personattributes_val_additionalattributes_val_fieldname = ' U-PAYROLL-OFF-NUM'
left outer join  prod_alight_trusted_tba.tba_datachangeevents_root_tbamessage_personbenefitaccountrelationships tbapbar
on rt.tbamessage_personbenefitaccountrelationships = tbapbar.id
and rt.partition_load_dt_tmstmp = tbapbar.partition_load_dt_tmstmp



