/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import doitincloud.commons.exceptions.ServerErrorException;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Utils {

    private static ExecutorService executor;

    public static ExecutorService getExcutorService() {
        if (executor == null) {
            executor = Executors.newCachedThreadPool();
        }
        return executor;
    }

    private static ObjectMapper mapper;

    public static ObjectMapper getObjectMapper() {

        if (null != mapper) return mapper;

        mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return mapper;
    }

    public static Map<String, Object> toMap(Object object) {
        if (object == null) return null;
        if (object instanceof String) {
            String json = (String) object;
            if (json.length() < 3) return null;
            json = json.trim();
            if (json.startsWith("{") && json.endsWith("}")) {
                try {
                    return getObjectMapper().readValue(json,
                            new TypeReference<LinkedHashMap<String, Object>>() {});
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (json.startsWith("[") && json.endsWith("]")) {
                try {
                    List<Object> list = getObjectMapper().readValue(json,
                            new TypeReference<ArrayList<Object>>() {});
                    if (list != null) {
                        return convertListToMap(list);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        } else if (object instanceof Map) {
            return new LinkedHashMap<>((Map) object);
        } else if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            return convertListToMap(list);
        } else {
            try {
                return getObjectMapper().convertValue(object, new TypeReference<LinkedHashMap<String, Object>>() {});
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static Map<String, Object> convertListToMap(List<Object> list) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        int i = 0;
        for (Object obj : list) {
            map.put(String.valueOf(i++), obj);
        }
        return map;
    }

    public static List<Object> toList(Object object) {
        if (object == null) return null;
        if (object instanceof String) {
            String json = (String) object;
            if (json.length() < 3) return null;
            json = json.trim();
            if (json.startsWith("{") && json.endsWith("}")) {
                try {
                    Map<String, Object> map = getObjectMapper().readValue(json,
                            new TypeReference<LinkedHashMap<String, Object>>() {});
                    if (map == null) {
                        return null;
                    }
                    return convertMapToList(map);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (json.startsWith("[") && json.endsWith("]")) {
                try {
                    List<Object> list = getObjectMapper().readValue(json, new TypeReference<ArrayList<Object>>() {});
                    return list;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return null;
        } else if (object instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) object;
            return convertMapToList(map);
        } else if (object instanceof List) {
            return new ArrayList<>((List) object);
        } else {
            try {
                return getObjectMapper().convertValue(object, new TypeReference<List<Object>>() {});
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public static List<Object> convertMapToList(Map<String, Object> map) {
        List<String> doneKeys = new ArrayList<>();
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < map.size(); i++) {
            String key = Integer.toString(i);
            Object value = map.get(key);
            list.add(value);
            if (value != null) {
                doneKeys.add(key);
            }
        }
        if (doneKeys.size() < map.size() ) {
            int i = 0;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                if (doneKeys.contains(key)) {
                    continue;
                }
                for (; i < list.size(); i++) {
                    if (list.get(i) == null) {
                        break;
                    }
                }
                if (i == list.size()) {
                    break;
                }
                list.set(i, entry);
            }
        }
        return list;
    }

    public static <T> T toPojo(Map<String, Object> map, Class<T> type) {
        try {
            ObjectMapper om = getObjectMapper();
            return om.convertValue(map, type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String toJson(Object object) {
        if (null == object) return null;
        try {
            return getObjectMapper().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String toJsonMap(Object object) {
        if (null == object) return null;
        try {
            if (object instanceof List) {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                int i = 0;
                for (Object obj : (List) object) {
                    map.put(String.valueOf(i++), obj);
                }
                return getObjectMapper().writeValueAsString(map);
            } else {
                return getObjectMapper().writeValueAsString(object);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String toPrettyJson(Object object) {
        try {
            return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static SimpleDateFormat yearFormat;

    public static SimpleDateFormat getYearFormat() {
        if (yearFormat == null) {
            yearFormat = new SimpleDateFormat("yyyy");
        }
        return yearFormat;
    }

    private static SimpleDateFormat timeFormat;

    public static SimpleDateFormat getTimeFormat() {
        if (timeFormat == null) {
            timeFormat = new SimpleDateFormat("HH:mm:ss");
        }
        return timeFormat;
    }

    private static SimpleDateFormat dateFormat;

    public static SimpleDateFormat getDateFormat() {
        if (dateFormat == null) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }
        return dateFormat;
    }

    private static SimpleDateFormat dateTimeFormat;

    public static SimpleDateFormat getDateTimeFormat() {
        if (dateTimeFormat == null) {
            dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            //dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        }
        return dateTimeFormat;
    }

    // 4c93c407-7915-4413-89e0-194b2a02314c
    // 01234567 8901 2345 6789 012345678901
    //            1            2         3
    public static String convertUuid(String s) {
        if (s.length() != 32) {
            throw new ServerErrorException("length != 32 convert, not supported");
        }
        return s.substring(0,8) + "-" +
                s.substring(8,12) + "-" +
                s.substring(12,16) + "-" +
                s.substring(16,20) + "-" +
                s.substring(20);
    }

    // 4c93c407-7915-4413-89e0-194b2a02314c
    // 012345678901234567890123456789012345
    //           1         2         3
    public static String uuidConvert(String uuid) {
        return uuid.substring(0,8) +
                uuid.substring(9,13) +
                uuid.substring(14,18) +
                uuid.substring(19,23) +
                uuid.substring(24,36);
    }

    public static String generateId() {
        String uuid = UUID.randomUUID().toString();
        return uuidConvert(uuid);
    }

    public static String generateUuid() {
        return UUID.randomUUID().toString();
    }
}
