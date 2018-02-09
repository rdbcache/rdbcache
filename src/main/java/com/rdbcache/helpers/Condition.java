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

import java.util.*;

public class Condition extends HashMap<String, List<String>> {

    public Condition(String ops, List<String> values) {
        push(ops, values);
    }

    public Condition(String ops, String[] values) {
        push(ops, values);
    }

    public Condition(List<String> values) {
        push(values);
    }

    public Condition(String[] values) {
        push(values);
    }

    public Condition(String ops, String value) {
        push(ops, value);
    }

    public Condition(String value) {
        push(value);
    }

    public Condition(Map<String, Object> map) {
        fromMap(map);
    }

    public Condition() {
    }

    public Map<String, List<String>> getMap() {
        return this;
    }

    public void push(String ops, List<String> values) {
        if (values == null || values.size() == 0) {
            return;
        }
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        } else {
            for(String value: values) {
                if (!list.contains(value)) {
                    list.add(value);
                }
            }
        }
    }

    public void push(String ops, String[] values) {
        if (values == null || values.length == 0) {
            return;
        }
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        }
        for (int i = 0; i< values.length; i++) {
            if (!list.contains(values[i])) {
                list.add(values[i]);
            }
        }
    }

    public void push(List<String> values) {
        if (values == null || values.size() == 0) {
            return;
        }
        String ops = "=";
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        }
        for(String value: values) {
            if (!list.contains(value)) {
                list.add(value);
            }
        }
    }

    public void push(String[] values) {
        if (values == null || values.length == 0) {
            return;
        }
        String ops = "=";
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        }
        for (int i = 0; i< values.length; i++) {
            if (!list.contains(values[i])) {
                list.add(values[i]);
            }
        }
    }

    public void push(String ops, String value) {
        if (value == null) {
            return;
        }
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        }
        if (!list.contains(value)) {
            list.add(value);
        }
    }

    public void push(String value) {
        if (value == null) {
            return;
        }
        String ops = "=";
        List<String> list = (List<String>) get(ops);
        if (list == null) {
            list = new ArrayList<String>();
            put(ops, list);
        }
        if (!list.contains(value)) {
            list.add(value);
        }
    }

    public Condition clone() {
        Condition condition = new Condition();
        for(Map.Entry<String, List<String>> entry: entrySet()) {
            List<String> values = entry.getValue();
            for (String value: values) {
                condition.push(entry.getKey(), value);
            }
        }
        return condition;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> newMap = new LinkedHashMap<String, Object>();
        for(Map.Entry<String, List<String>> entry: entrySet()) {
            List<String> values = (List<String>) entry.getValue();
            List<String> list = new ArrayList<String>();
            for (String value: values) {
                list.add(value);
            }
            newMap.put(entry.getKey(), list);
        }
        return newMap;
    }

    public void fromMap(Map<String, Object> mapNew) {
        if (mapNew == null || mapNew.size() == 0) return;
        for (Map.Entry<String, Object> entry: mapNew.entrySet()) {
            List<String> values = (List<String>) entry.getValue();
            List<String> list = new ArrayList<String>();
            for (String value: values) {
                list.add(value);
            }
            push(entry.getKey(), list);
        }
    }
}
