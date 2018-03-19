package com.rdbcache.repositories;

import com.rdbcache.models.KvIdType;
import com.rdbcache.models.KvPair;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleKvPairRepo implements KvPairRepo {

    private Map<String, Object> data = new LinkedHashMap<>();

    @Override
    public <S extends KvPair> S save(S entity) {
        String key = entity.getId();
        //Map<String, Object> map = entity.toMap();
        return null;
    }

    @Override
    public <S extends KvPair> Iterable<S> save(Iterable<S> entities) {
        return null;
    }

    @Override
    public KvPair findOne(KvIdType kvIdType) {
        return null;
    }

    @Override
    public boolean exists(KvIdType kvIdType) {
        return false;
    }

    @Override
    public Iterable<KvPair> findAll() {
        return null;
    }

    @Override
    public Iterable<KvPair> findAll(Iterable<KvIdType> kvIdTypes) {
        return null;
    }

    @Override
    public long count() {
        return data.size();
    }

    @Override
    public void delete(KvIdType kvIdType) {

    }

    @Override
    public void delete(KvPair entity) {

    }

    @Override
    public void delete(Iterable<? extends KvPair> entities) {

    }

    @Override
    public void deleteAll() {

    }
}
