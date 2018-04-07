This README serves as a quick start docuement. For more detailed documentation, please visit website: http://rdbcache.com

Install rdbcache
----------------

rdbcache is a java application. It requires Java version 1.8+ runtime environment.

#### For Mac/Linux:

    curl https://raw.githubusercontent.com/rdbcache/rdbcache/master/download/install | sh

    # check if OK
    rdbcache -v

    Put following environment variables in your ~/.bash_profile.
    Please replace the values with the proper ones for your environment.

    export RDBCACHE_PORT=8181
    export REDIS_SERVER=localhost
    export DATABASE_NAME=testdb
    export DATABASE_SERVER=localhost
    export DB_USER_NAME=dbuser
    export DB_USER_PASS=rdbcache

    rdbcache

#### For Windows:

click [download rdbcache.zip](https://raw.githubusercontent.com/rdbcache/rdbcache/master/download/rdbcache.zip)

    # check if OK
    java -jar rdbcache.jar -v

    Please replace the values with the proper ones for your environment.

    SET RDBCACHE_PORT=8181
    SET REDIS_SERVER=localhost
    SET DATABASE_NAME=testdb
    SET DATABASE_SERVER=localhost
    SET DB_USER_NAME=dbuser
    SET DB_USER_PASS=rdbcache

    java -jar rdbcache.jar


Playing with rdbcache
---------------------

    curl http://localhost:8181/v1/set/my-hash-key/my-value
    {"timestamp":1518188737321,"duration":"0.00639","key":"my-hash-key","trace_id":"5554d502d58448e0a137196af0a4b1fa"}

    curl http://localhost:8181/v1/get/my-hash-key
    {"timestamp":1518188797475,"duration":"0.000454","key":"my-hash-key","data":"my-value","trace_id":"6f7c05360ec74b12bda80ec692030031"}

    curl http://localhost:8181/v1/select/rdbcache_monitor?limit=3
    ...

    curl http://localhost:8181/v1/select/rdbcache_stopwatch?limit=3
    ...


You can find all the available APIs and complete documentation at http://rdbcache.com.

Playing with source code
------------------------

rdbcache uses maven and build on top of Java Spring Boot 1.5.10. It requires maven 3.5+ and JDK version 1.8+.

Download source from github

git clone https://github.com/rdbcache/rdbcache.git

cd rdbcache

mvn clean test

mvn clean spring-boot:run

mvn clean test package