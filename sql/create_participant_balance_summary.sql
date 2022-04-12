drop table datamart.participant_balance_summary;
create table datamart.participant_balance_summary(
  participant_id varchar(1000),
  global_participant_id varchar(1000),
  record_start_date date,
  record_end_date date,
  current_indicator bigint,
  create_date date,
  update_date date,
  update_userid varchar(10),
  participant_name varchar(1000),
  ssn varchar(1000),
  fundId varchar(100),
  subfundid varchar(100),
  accountId varchar(100),
  fund_desc varchar(100),
  fund_balance_units decimal(22,7),
  price varchar(100),
  civilian_group varchar(100),
  uniformed_group varchar(100)
) DISTKEY (participant_id);
