drop table datamart.participant_balance_summary;
create table datamart.participant_balance_summary(
  participant_id varchar(100),
  global_participant_id varchar(100),
  record_start_date date,
  record_end_date date,
  current_indicator bigint,
  create_date date,
  update_date date,
  update_userid varchar(10),
  participant_name varchar(100),
  ssn varchar(100),
  fundId varchar(10),
  subfundid varchar(10),
  accountId varchar(10),
  fund_balance_units varchar(10),
  price varchar(10),
  civilian_group varchar(10),
  uniformed_group varchar(100)
) DISTKEY (participant_id);
