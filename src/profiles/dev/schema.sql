CREATE TABLE IF NOT EXISTS rdbcache_kv_pair (
  id varchar(255) not null,
  type varchar(16) not null,
  value text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
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

CREATE TABLE IF NOT EXISTS dept_emp (
    emp_no      INT             NOT NULL,
    dept_no     CHAR(4)         NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    KEY         (emp_no),
    KEY         (dept_no),
    FOREIGN KEY (emp_no)  REFERENCES employees   (emp_no)  ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,dept_no)
);

CREATE TABLE IF NOT EXISTS dept_manager (
   dept_no      CHAR(4)         NOT NULL,
   emp_no       INT             NOT NULL,
   from_date    DATE            NOT NULL,
   to_date      DATE            NOT NULL,
   KEY         (emp_no),
   KEY         (dept_no),
   FOREIGN KEY (emp_no)  REFERENCES employees (emp_no)    ON DELETE CASCADE,
   FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
   PRIMARY KEY (emp_no,dept_no)
);

CREATE TABLE IF NOT EXISTS titles (
    emp_no      INT             NOT NULL,
    title       VARCHAR(50)     NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,title, from_date)
);

CREATE TABLE IF NOT EXISTS salaries (
    emp_no      INT             NOT NULL,
    salary      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, from_date)
);

CREATE TABLE IF NOT EXISTS departments2 (
    dept_no     CHAR(4)         NOT NULL,
    dept_name   VARCHAR(40)     NOT NULL,
    PRIMARY KEY (dept_no),
    UNIQUE  KEY (dept_name)
);

CREATE TABLE IF NOT EXISTS employees2 (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

CREATE TABLE IF NOT EXISTS dept_emp2 (
    emp_no      INT             NOT NULL,
    dept_no     CHAR(4)         NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    KEY         (emp_no),
    KEY         (dept_no),
    FOREIGN KEY (emp_no)  REFERENCES employees2   (emp_no)  ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments2 (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,dept_no)
);

CREATE TABLE IF NOT EXISTS dept_manager2 (
   dept_no      CHAR(4)         NOT NULL,
   emp_no       INT             NOT NULL,
   from_date    DATE            NOT NULL,
   to_date      DATE            NOT NULL,
   KEY         (emp_no),
   KEY         (dept_no),
   FOREIGN KEY (emp_no)  REFERENCES employees2 (emp_no)    ON DELETE CASCADE,
   FOREIGN KEY (dept_no) REFERENCES departments2 (dept_no) ON DELETE CASCADE,
   PRIMARY KEY (emp_no,dept_no)
);

CREATE TABLE IF NOT EXISTS titles2 (
    emp_no      INT             NOT NULL,
    title       VARCHAR(50)     NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees2 (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no,title, from_date)
);

CREATE TABLE IF NOT EXISTS salaries2 (
    emp_no      INT             NOT NULL,
    salary      INT             NOT NULL,
    from_date   DATE            NOT NULL,
    to_date     DATE            NOT NULL,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees2 (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, from_date)
);

CREATE TABLE IF NOT EXISTS tb1 (
  id int not null auto_increment,
  name varchar(16),
  age integer,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS tb2 (
  id varchar(16) not null,
  name varchar(32),
  dob date,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS user_table (
  id int not null auto_increment,
  email varchar(255),
  name varchar(32),
  dob date,
  PRIMARY KEY (id),
  UNIQUE KEY (email)
);