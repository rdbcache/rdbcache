DROP TABLE IF EXISTS rdbcache_kv_pair;

CREATE TABLE IF NOT EXISTS rdbcache_kv_pair (
  id varchar(255) not null,
  type varchar(255) not null,
  value text,
  PRIMARY KEY (id, type)
);

DROP TABLE IF EXISTS rdbcache_monitor;

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

DROP TABLE IF EXISTS rdbcache_stopwatch;

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

DROP TABLE IF EXISTS user_table;

CREATE TABLE IF NOT EXISTS user_table (
  id int not null auto_increment,
  email varchar(255),
  name varchar(32),
  dob date,
  PRIMARY KEY (id),
  UNIQUE KEY (email)
);

DROP TABLE IF EXISTS employees;

CREATE TABLE IF NOT EXISTS employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

DROP TABLE IF EXISTS user_table2;

CREATE TABLE IF NOT EXISTS user_table2 (
  id int not null auto_increment,
  email varchar(255),
  name varchar(32),
  dob date,
  PRIMARY KEY (id),
  UNIQUE KEY (email)
);
