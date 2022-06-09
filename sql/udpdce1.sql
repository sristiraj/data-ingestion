udpdce as (select * from (select idm.message_body_update_after_modificationhistory_val_sourcesysteminternalid  ,
udp.message_header_action,
udp.message_body_update_after_globalpersonidentifier ,
udp.message_header_messagetimestamp,
row_number() over(partition by idm.message_body_update_after_modificationhistory_val_sourcesysteminternalid order by 
to_timestamp(case when udp.message_header_messagetimestamp="" then null else udp.message_header_messagetimestamp end ,"yyyy-MM-dd'T'HH:mm:ss.SSSz") desc) rn
from 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root_message_body_update_after_modificationhistory) idm 
left outer join 
(select * from prod_alight_trusted_tba.tba_udpdatachangeevents_root ) udp 
on udp.message_body_create_idmapping = idm.id and udp.partition_load_dt_tmstmp = idm.partition_load_dt_tmstmp
)
where rn=1)
