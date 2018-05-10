CREATE TABLE IF NOT EXISTS rdbcache_kv_pair (
  id varchar(255) not null,
  type varchar(255) not null,
  value text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id, type)
);

CREATE TABLE IF NOT EXISTS rdbcache_monitor (
  id bigint not null auto_increment,
  name varchar(255) not null,
  thread_id int,
  duration bigint,
  main_duration bigint,
  client_duration bigint,
  started_at bigint,
  ended_at bigint,
  trace_id varchar(64),
  built_info varchar(255),
  KEY (name),
  KEY (trace_id),
  KEY (built_info),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rdbcache_client_test (
  id bigint not null auto_increment,
  trace_id varchar(64),
  status varchar(32),
  passed boolean,
  verify_passed boolean,
  duration bigint,
  process_duration bigint,
  route varchar(255),
  url varchar(255),
  data text,
  KEY (trace_id),
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rdbcache_stopwatch (
  id bigint not null auto_increment,
  monitor_id bigint not null,
  type varchar(16) not null,
  action varchar(255),
  thread_id int,
  duration bigint,
  started_at bigint,
  ended_at bigint,
  KEY (monitor_id),
  PRIMARY KEY(id),
  FOREIGN KEY(monitor_id) REFERENCES rdbcache_monitor(id) ON DELETE CASCADE ON UPDATE CASCADE
);
