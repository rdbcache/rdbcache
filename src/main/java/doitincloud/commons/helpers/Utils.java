/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.helpers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import doitincloud.commons.exceptions.BadRequestException;

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
        List<Object> list = new ArrayList<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            String key = Integer.toString(i);
            Object value = map.get(key);
            if (value != null) {
                list.set(i, value);
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
            return getObjectMapper().convertValue(map, type);
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
            //dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
            dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        }
        return dateTimeFormat;
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

    // loosely check if object a equals object b
    //
    public static boolean isValueEquals(Object a, Object b) {

        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }

        // equals
        if (a == b || a.equals(b)) {
            return true;
        }

        // String equals
        String as = a.toString().trim();
        String bs = b.toString().trim();
        if (as.equals(bs)) {
            return true;
        }

        // class name is same, and reached here
        String acn = a.getClass().getSimpleName();
        String bcn = b.getClass().getSimpleName();
        if (acn.equals(bcn)) {
            return false;
        }

        // boolean related
        if (acn.equals("Boolean") || bcn.equals("Boolean")) {
            Boolean ab = null;
            if (acn.equals("Boolean")) {
                ab = (Boolean) a;
            } else if (Arrays.asList("1", "true", "TRUE").contains(as)) {
                ab = true;
            } else if (Arrays.asList("0", "false", "FALSE").contains(as)) {
                ab = false;
            }
            Boolean bb = null;
            if (bcn.equals("Boolean")) {
                bb = (Boolean) b;
            } else if (Arrays.asList("1", "true", "TRUE").contains(bs)) {
                bb = true;
            } else if (Arrays.asList("0", "false", "FALSE").contains(bs)) {
                bb = false;
            }
            if (ab != null && bb != null && ab.equals(bb)) {
                return true;
            }
        }

        // decimal related
        if (acn.equals("Double") || acn.equals("Float") || acn.equals("BigDecimal") ||
                bcn.equals("Double") || bcn.equals("Float") || bcn.equals("BigDecimal")) {
            Double ad = null;
            try {
                ad = Double.valueOf(as);
            } catch (Exception e) {
                //e.printStackTrace();
            }
            Double bd = null;
            try {
                bd = Double.valueOf(bs);
            } catch (Exception e) {
                //e.printStackTrace();
            }
            if (ad != null && bd != null && ad.equals(bd)) {
                return true;
            }
        }

        // number related
        if (acn.equals("Long") || acn.equals("Integer") ||
                bcn.equals("Long") || bcn.equals("Integer")) {
            Long al = null;
            try {
                al = Long.valueOf(as);
            } catch (Exception e) {
                //e.printStackTrace();
            }
            Long bl = null;
            try {
                bl = Long.valueOf(bs);
            } catch (Exception e) {
                //e.printStackTrace();
            }
            if (al != null && bl != null && al.equals(bl)) {
                return true;
            }
        }
        return false;
    }

    // get changes of map after use update to update map, but map keeps untouched
    // return true if key set of update is a subset of key set of map
    //
    public static boolean mapChangesAfterUpdate(Map<String, Object> update, Map<String, Object> map, Map<String, Object> changes) {

        if (update == null || map == null) {
            return false;
        }

        Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
        changes.clear();
        for (Map.Entry<String, Object> entry : update.entrySet()) {

            String key = entry.getKey();
            if (!map.containsKey(key)) {
                changes.put(key, entry.getValue()); // for report purpose
                return false;
            }
            Object value = entry.getValue();
            if (!isValueEquals(value, map.get(key))) {
                changes.put(key, value);
            }
        }
        return true;
    }

    // check if map a loosely equals to map b
    //
    public static boolean isMapEquals(Map<String, Object> a, Map<String, Object> b) {

        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else if (a.size() != b.size()) {
            return false;
        }

        for (Map.Entry<String, Object> entry : a.entrySet()) {
            String key = entry.getKey();
            if (!isValueEquals(entry.getValue(), b.get(key))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isMapEquals(String a, Map<String, Object> b) {
        return isMapEquals(toMap(a), b);
    }

    public static boolean isMapEquals(Map<String, Object> a, String b) {
        return isMapEquals(a, toMap(b));
    }

    public static boolean isMapEquals(String a, String b) {
        return isMapEquals(toMap(a), toMap(b));
    }
}