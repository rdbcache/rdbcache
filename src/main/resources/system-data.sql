delete from rdbcache_kv_pair;

insert into rdbcache_kv_pair (id, type, value) values('id1', 'data', 'value1');
insert into rdbcache_kv_pair (id, type, value) values('id2', 'data', '{"f1":"v21"}');
insert into rdbcache_kv_pair (id, type, value) values('id3', 'data', '{"f1":"v31"}');
insert into rdbcache_kv_pair (id, type, value) values('id4', 'data', '{"f1":"v41","f2":"v42","f3":null}');
