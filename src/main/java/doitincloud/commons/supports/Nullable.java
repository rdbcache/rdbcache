/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.supports;

import org.springframework.data.redis.serializer.GenericToStringSerializer;

public class Nullable extends GenericToStringSerializer<Object> {

    private static byte[] bytesNull = "null".getBytes();

    public Nullable() {
        super(Object.class);
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (bytesNull.equals(bytes)) {
            return null;
        }
        return super.deserialize(bytes);
    }

    @Override
    public byte[] serialize(Object object) {
        if (object == null) return bytesNull;
        return super.serialize(object);
    }
}
