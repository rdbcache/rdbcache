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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.*;

public class Utils {

    private static ObjectMapper mapper;

    public static ObjectMapper getObjectMapper() {
        if (null != mapper) return mapper;
        mapper = new ObjectMapper();
        return mapper;
    }

    public static Map<String, Object> toMap(String json) {
        if (null == json || json.length() < 3) return null;
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            try {
                return getObjectMapper().readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (json.startsWith("[") && json.endsWith("]")) {
            try {
                List<Object> list = getObjectMapper().readValue(json, new TypeReference<ArrayList<Object>>() {
                });
                if (list != null) {
                    Map<String, Object> map = new LinkedHashMap<String, Object>();
                    int i = 0;
                    for (Object object : list) {
                        map.put(String.valueOf(i++), object);
                    }
                    return map;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    public static String toPrettyJson(Object o) {
        try {
            return getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 4c93c407-7915-4413-89e0-194b2a02314c
    // 012345678901234567890123456789012345
    //           1         2         3
    public static String generateId() {
        String uuid = UUID.randomUUID().toString();
        return uuid.substring(0,8) +
                uuid.substring(9,13) +
                uuid.substring(14,18) +
                uuid.substring(19,23) +
                uuid.substring(24,36);
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
            if (bcn.equals("java.lang.Boolean")) {
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
                e.printStackTrace();
            }
            Double bd = null;
            try {
                bd = Double.valueOf(bs);
            } catch (Exception e) {
                e.printStackTrace();
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
                e.printStackTrace();
            }
            Long bl = null;
            try {
                bl = Long.valueOf(bs);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (al != null && bl != null && al.equals(bl)) {
                return true;
            }
        }

        return false;
    }

    // get changes of dbMap after use inputMap to update dbMap, but dbMap keeps untouched
    // return true if inputMap is a subset of dbMap and work done
    //
    public static boolean MapChangesAfterUpdate(Map<String, Object> inputMap, Map<String, Object> dbMap, Map<String, Object> changes) {

        if (inputMap == null || dbMap == null) {
            return false;
        }

        Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
        changes.clear();
        for (Map.Entry<String, Object> entry : inputMap.entrySet()) {

            String key = entry.getKey();
            if (!dbMap.containsKey(key)) {
                return false;
            }
            Object aval = entry.getValue();
            Object bval = dbMap.get(key);
            if (aval == null && bval == null) {
                continue;
            } else if (aval == null || bval == null) {
                changes.put(key, aval);
            } else if (!isValueEquals(aval, bval)) {
                changes.put(key, aval);
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
        }

        for (Map.Entry<String, Object> entry : a.entrySet()) {

            String key = entry.getKey();
            Object aval = entry.getValue();
            Object bval = b.get(key);
            if (aval == null && bval == null) {
                continue;
            } else if (aval == null || bval == null) {
                return false;
            } else if (!isValueEquals(aval, bval)) {
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
