package com.rdbcache.repositories;

import com.rdbcache.models.Monitor;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleMonitorRepo implements MonitorRepo {

    private Map<String, Object> data = new LinkedHashMap<>();

    @Override
    public Monitor findByTraceId(String traceId) {
        return null;
    }

    long count = 1000l;

    @Override
    public <S extends Monitor> S save(S entity) {
        entity.setId(count++);
        return entity;
    }

    @Override
    public <S extends Monitor> Iterable<S> save(Iterable<S> entities) {
        return null;
    }

    @Override
    public Monitor findOne(Long aLong) {
        return null;
    }

    @Override
    public boolean exists(Long aLong) {
        return false;
    }

    @Override
    public Iterable<Monitor> findAll() {
        return null;
    }

    @Override
    public Iterable<Monitor> findAll(Iterable<Long> longs) {
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
    public void delete(Monitor entity) {

    }

    @Override
    public void delete(Iterable<? extends Monitor> entities) {

    }

    @Override
    public void deleteAll() {

    }
}
