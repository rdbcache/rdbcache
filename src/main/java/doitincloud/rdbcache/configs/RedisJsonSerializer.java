package doitincloud.rdbcache.configs;

import com.fasterxml.jackson.core.JsonProcessingException;

import doitincloud.commons.helpers.Utils;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class RedisJsonSerializer implements RedisSerializer<Object> {

    private static byte[] bytesNull = "null".getBytes();

    @Override
    public byte[] serialize(Object o) throws SerializationException {
        if (o == null) {
            return bytesNull;
        }
        try {
            return Utils.getObjectMapper().writeValueAsBytes(o);
        } catch (JsonProcessingException e) {
            throw new SerializationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) throws SerializationException {
        if (bytes == null) {
            return null;
        }
        if (bytesNull.equals(bytes)) {
            return null;
        }
        try {
            return Utils.getObjectMapper().readValue(bytes, Object.class);
        } catch (Exception e) {
            throw new SerializationException(e.getMessage(), e);
        }
    }
}
