package doitincloud.rdbcache.configs;

import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.models.KeyInfo;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.*;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;

public class MockRedis {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockRedis.class);

    private static Map<String, Object> data = new HashMap<>();

    public static Map<String, Object> getData() {
        return data;
    }

    public static StringRedisTemplate mockStringRedisTemplate() {

        StringRedisTemplate template = mock(StringRedisTemplate.class, Mockito.RETURNS_DEEP_STUBS);

        // mock StringRedisTemplate delete
        //
        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Set<String> keys = (Set<String>) args[0];
            LOGGER.trace("StringRedisTemplate delete " + keys);
            for (String key : keys) {
                data.remove(key);
            }
            return null;

        }).when(template).delete(anySet());

        HashOperations hashOps = mock(HashOperations.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(template.opsForHash()).thenReturn(hashOps);

        // mock HashOperations putAll
        //
        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String key = (String) args[0];
            Map<String, Object> map = (Map<String, Object>) args[1];
            LOGGER.trace("StringRedisTemplate HashOperations putAll " + key + " " + map.keySet());
            Map<String, Object> wholeMap = (Map<String, Object>) data.get(key);
            if (wholeMap == null) {
                wholeMap = new LinkedHashMap<>();
                data.put(key, wholeMap);
            }
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                wholeMap.put(entry.getKey(), entry.getValue());
            }
            return null;
        }).when(hashOps).putAll(anyString(), anyMap());


        // mock HashOperations entries
        //
        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String key = (String) args[0];
            LOGGER.trace("StringRedisTemplate HashOperations entries " + key);
            Map<String, Object> map = (Map<String, Object>) data.get(key);
            if (map == null) {
                return null;
            }
            Map<String, Object> mapClone = new LinkedHashMap<>(map);
            return mapClone;
        }).when(hashOps).entries(anyString());

        // opsForValue only use in ExpireOps for lua scripts, set it to null to bypass the real operations
        //
        ValueOperations valueOps =  mock(ValueOperations.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(template.opsForValue()).thenReturn(valueOps);

        // mock ValueOperations set
        //
        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String key = (String) args[0];
            String expire = (String) args[1];
            Integer expValue = (Integer) args[2];
            LOGGER.trace("StringRedisTemplate ValueOperations set " + key + " " + expire + " " + expValue);
            data.put(key, Arrays.asList(expire, expValue));
            return null;
        }).when(valueOps).set(anyString(), anyString(), anyInt());

        // mock ValueOperations get
        //
        Mockito.doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            String key = (String) args[0];
            if ("__is_mock_test__".equals(key)) {
                return "__TRUE__";
            }
            LOGGER.trace("StringRedisTemplate ValueOperations get " + key );
            return data.get(key);
        }).when(valueOps).get(anyString());

        // mock (ListOperations leftPop
        //
        ListOperations listOps = mock(ListOperations.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(template.opsForList()).thenReturn(listOps);

        Mockito.when(listOps.leftPop(anyString(), anyLong(), anyObject())).thenAnswer(invocation -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return null;
        });

        return template;
   }

   public static RedisKeyInfoTemplate mockKeyInfoRedisTemplate() {

       RedisKeyInfoTemplate template = mock(RedisKeyInfoTemplate.class, Mockito.RETURNS_DEEP_STUBS);

       HashOperations keyInfoOps = mock(HashOperations.class, Mockito.RETURNS_DEEP_STUBS);

       Mockito.when(template.opsForHash()).thenReturn(keyInfoOps);

       // mock HashOperations get
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           String subKey = (String) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations get " + key + " " + subKey);
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           if (map == null) {
               return null;
           }
           Map<String, Object> subMap =  (Map<String, Object>) map.get(subKey);
           return Utils.toPojo(subMap, KeyInfo.class);
       }).when(keyInfoOps).get(anyString(), anyString());

       // mock HashOperations put
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           String subKey = (String) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations put " + key + " " + subKey);
           KeyInfo keyInfo = (KeyInfo) args[2];
           Map<String, Object> subMap = Utils.toMap(keyInfo);
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           if (map == null) {
               map = new LinkedHashMap<>();
               data.put(key, map);
           }
           map.put(subKey, subMap);
           return null;
       }).when(keyInfoOps).put(anyString(), anyString(), any(KeyInfo.class));

       // mock HashOperations putAll
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           Map<String, Object> subMaps = (Map<String, Object>) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations putAll " + key + " " + subMaps.keySet());
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           if (map == null) {
               map = new LinkedHashMap<>();
               data.put(key, map);
           }
           for (Map.Entry<String, Object> entry : subMaps.entrySet()) {
               String subKey = entry.getKey();
               KeyInfo keyInfo = (KeyInfo) entry.getValue();
               map.put(subKey, Utils.toMap(keyInfo));
           }
           return null;
       }).when(keyInfoOps).putAll(anyString(), anyMap());

       // mock HashOperations multiGet
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           List<String> keys = (List<String>) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations multiGet " + key);
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           List<KeyInfo> resultList = new ArrayList<>();
           if (map == null) {
               return resultList;
           }
           for (String subKey: keys) {
               Map<String, Object> subMap = (Map<String, Object>) map.get(subKey);
               if (subMap != null) {
                   KeyInfo keyInfo = Utils.toPojo(subMap, KeyInfo.class);
                   resultList.add(keyInfo);
               } else {
                   resultList.add(null);
               }
           }
           return resultList;
       }).when(keyInfoOps).multiGet(anyString(), anyList());

       // mock HashOperations delete single
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           String subKey = (String) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations delete " + key + " " + subKey);
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           if (map == null) {
               return null;
           }
           map.remove(subKey);
           return null;
       }).when(keyInfoOps).delete(anyString(), anyString());


       // mock HashOperations delete multiple
       //
       Mockito.doAnswer(invocation -> {
           Object[] args = invocation.getArguments();
           String key = (String) args[0];
           Map<String, Object> map = (Map<String, Object>) data.get(key);
           if (map == null) {
               return null;
           }
           List<String> keys = (List<String>) args[1];
           LOGGER.trace("RedisKeyInfoTemplate HashOperations delete " + key + " " + keys);
           for (String subKey: keys) {
               map.remove(subKey);
           }
           return null;
       }).when(keyInfoOps).delete(anyString(), anyList());

       return template;
   }
}
