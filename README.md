This README serves as a quick start docuement. For more detailed documentation, please visit website: http://rdbcache.com

What is rdbcache?
----------------

rdbcache stands for redis database cache. It is an open source database cache server. rdbcache uses redis as cache to offers asynchronous cache api service. It provides eventually consistency between redis and database. rdbcache attempts to bridge the gap between redis and database.

The asynchronous nature makes rdbcache very fast and useful in many scenarios. Through few simple restful APIs , rdbcache offers the convenience for developers to easily take advantage of the powers and benefits of both redis and database.

Install rdbcache
----------------

Download rdbcache.tar.gz

https://github.com/rdbcache/rdbcache/blob/master/download/rdbcache.tar.gz

tar xvf rdbcache.tar.gz

cd rdbcache

./rdbcache -v

Configure redis server and mysql data source

setup environment variables:

export REDIS_SERVER=

export DATABASE_NAME=

export DATABASE_SERVER=

export DB_USER_NAME=

export DB_USER_PASS=

Change server port

The default server listening port is 8181. To change it:

vi application.properties

server.port=NEW_PORT

run rdbcache in background

./rdbcache &

install and run rdbcache as service

./install_service

Build rdbcache
--------------

rdbcache uses maven and build on top of Java Spring Boot 1.5.10. It requires maven 3.5+ and Java SDK 1.8+.

Download source from github

git clone https://github.com/rdbcache/rdbcache.git

cd rdbcache

mvn -P prod clean package

./build

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

You can find all the available APIs at http://rdbcache.com.