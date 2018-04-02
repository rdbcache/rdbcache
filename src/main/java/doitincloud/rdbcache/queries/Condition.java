/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.rdbcache.queries;

import java.util.*;

public class Condition extends LinkedHashMap<String, List<String>> {

    public Condition(String ops, String[] values) {
        push(ops, values);
    }

    public Condition() {
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
}
