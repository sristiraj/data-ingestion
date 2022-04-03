select distinct rt.tbamessage_header_platforminternalid as participant_id,
rt.tbamessage_header_globalPersonIdentifier as global_participant_id,
current_date as record_start_date,
to_date('9999-09-09','yyyy-MM-dd') as record_end_date,
cast(1 as long) as current_indicator,
current_date create_date,
current_date as update_date,
'glue' as update_userid,
cast(null as string) as participant_name,
rt.tbamessage_header_nationaltaxid ssn,
tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_fundid fundId,
tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_subfundid subfundid ,
tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid accountId,
lfp.price,
tbpavaa.tbamessage_personattributes_val_additionalattributes_val_fieldname payroll_office_number,
ltam_civ.`group` civilian_group,
ltam_uni.`group` uniformed_group
from default.tba_json_balance_summary_root rt left outer join default.tba_json_balance_summary_root rt_acc
on rt.tbamessage_header_platforminternalid = rt_acc.message_header_platforminternalid
and rt_acc.message_header_subtopic = 'DCUnitsRORContributions' 
and rt.tbamessage_header_subtopic='PersonWorker'
left outer join default.tba_json_balance_summary_root_message_definedcontributionbenefits_definedcontributionplans tbadcbdcp
on rt_acc.message_definedcontributionbenefits_definedcontributionplans = tbadcbdcp.id
left outer join tba_json_balance_summary_root_message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable tbadcbdcppft
on tbadcbdcp.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable = tbadcbdcppft.id
left outer join default.tba_balance_summary_root_tbamessage_personattributes tbpa 
on rt.tbamessage_personattributes=tbpa.id left outer join 
default.tba_balance_summary_root_tbamessage_personattributes_val_additionalattributes tbpavaa
on tbpa.tbamessage_personattributes_val_additionalattributes = tbpavaa.id
left outer join default.lkp_fund_price lfp 
on cast(tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_fundid as varchar(10)) = cast(lfp.fundId as varchar(10))
left outer join default.lkp_tba_account_mapping ltam_civ on 
 cast(ltam_civ.tba_accountid_civilian as varchar(100))=cast(tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid as varchar(100))
left outer join  default.lkp_tba_account_mapping ltam_uni on 
cast(ltam_uni.tba_accountid_uniformed as varchar(100))=cast(tbadcbdcppft.message_definedcontributionbenefits_definedcontributionplans_val_planaccountfundtable_val_accountid as varchar(100))
