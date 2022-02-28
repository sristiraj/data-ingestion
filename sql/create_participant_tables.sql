drop table datamart.participant_5001_stg;
create table datamart.participant_5001_stg(
  participant_id varchar(100),
  global_participant_id varchar(100),
  record_start_date date,
  record_end_date date,
  current_indicator bigint,
  hash_key varchar(1000),
  create_date date,
  update_date date,
  update_userid varchar(10),
  participant_name varchar(100),
  ssn varchar(100),
  birth_date varchar(100),
  age bigint,
  department_code varchar(10),
  employer_agency_code varchar(10),
  payroll_office_number varchar(10),
  personnel_office_indicator varchar(10),
  employment_status varchar(100),
  auto_enrollment_code varchar(50),
  auto_enrollment_date varchar(100)
) DISTKEY (hash_key);

drop table datamart.participant_5001_hist;
create table datamart.participant_5001_hist(
  participant_id varchar(100),
  global_participant_id varchar(100),
  record_start_date date,
  record_end_date date,
  current_indicator bigint,
  hash_key varchar(1000),
  create_date date,
  update_date date,
  update_userid varchar(10),
  participant_name varchar(100),
  ssn varchar(100),
  birth_date varchar(100),
  age bigint,
  department_code varchar(10),
  employer_agency_code varchar(10),
  payroll_office_number varchar(10),
  personnel_office_indicator varchar(10),
  employment_status varchar(100),
  auto_enrollment_code varchar(50),
  auto_enrollment_date varchar(100)
) DISTKEY (hash_key);


