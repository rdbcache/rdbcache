package com.rdbcache.repositories;

import com.rdbcache.models.StopWatch;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleStopWatchRepo implements StopWatchRepo {

    private Map<String, Object> data = new LinkedHashMap<>();

    long count = 2000l;

    @Override
    public <S extends StopWatch> S save(S entity) {
        entity.setId(count++);
        return entity;
    }

    @Override
    public <S extends StopWatch> Iterable<S> save(Iterable<S> entities) {
        return null;
    }

    @Override
    public StopWatch findOne(Long aLong) {
        return null;
    }

    @Override
    public boolean exists(Long aLong) {
        return false;
    }

    @Override
    public Iterable<StopWatch> findAll() {
        return null;
    }

    @Override
    public Iterable<StopWatch> findAll(Iterable<Long> longs) {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public void delete(Long aLong) {

    }

    @Override
    public void delete(StopWatch entity) {

    }

    @Override
    public void delete(Iterable<? extends StopWatch> entities) {

    }

    @Override
    public void deleteAll() {

    }
}
