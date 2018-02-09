/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
