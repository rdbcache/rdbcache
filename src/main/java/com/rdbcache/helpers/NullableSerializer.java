/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package com.rdbcache.helpers;

import org.springframework.data.redis.serializer.GenericToStringSerializer;

public class NullableSerializer extends GenericToStringSerializer<Object> {

    private static byte[] bnull = "null".getBytes();

    public NullableSerializer() {
        super(Object.class);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (bnull.equals(bytes)) {
            return null;
        }
        return super.deserialize(bytes);
    }

    @Override
    public byte[] serialize(Object object) {
        if (object == null) return bnull;
        return super.serialize(object);
    }
}
