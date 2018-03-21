CREATE TABLE IF NOT EXISTS rdbcache_kv_pair (
  id varchar(255) not null,
  type varchar(16) not null,
  value text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id, type)
);

CREATE TABLE IF NOT EXISTS rdbcache_monitor (
  id int not null auto_increment,
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
  id int not null auto_increment,
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
  id int not null auto_increment,
  monitor_id int not null,
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

CREATE TABLE IF NOT EXISTS departments (
    dept_no     CHAR(4)         NOT NULL,
    dept_name   VARCHAR(40)     NOT NULL,
    PRIMARY KEY (dept_no),
    UNIQUE  KEY (dept_name)
);

CREATE TABLE IF NOT EXISTS employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

CREATE TABLE IF NOT EXISTS user_table (
  id int not null auto_increment,
  email varchar(255),
  name varchar(32),
  dob date,
  PRIMARY KEY (id),
  UNIQUE KEY (email)
);

CREATE TABLE IF NOT EXISTS types_table (
  id int not null auto_increment,
  type_boolean boolean,
  type_integer integer,
  type_int int,
  type_tinyint tinyint,
  type_smallint smallint,
  type_bigint bigint,
  type_decimal decimal(20,2),
  type_double double,
  type_real real,
  type_time time,
  type_date date,
  type_datetime datetime,
  type_timestamp timestamp,
  type_year year,
  type_char char,
  type_char32 char(32),
  type_varchar32 varchar(32),
  type_binary binary,
  type_blob blob,
  type_text text,
  PRIMARY KEY (id)
);
