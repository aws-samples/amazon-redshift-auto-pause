create table if not exists autopause_log (
  log_ts datetime,
  query_cnt int,
  status varchar(10));
