package doitincloud.rdbcache.supports;

import doitincloud.commons.helpers.Utils;

import java.text.SimpleDateFormat;
import java.util.*;

public class DbUtils {

    public static String formatDate(String type, Date date) {
        if (type.startsWith("year") || type.equals("year")) {
            SimpleDateFormat sdf = Utils.getYearFormat();
            return sdf.format(date);
        } else if (type.equals("time")) {
            SimpleDateFormat sdf = Utils.getTimeFormat();
            return sdf.format(date);
        } else if (type.equals("date")) {
            SimpleDateFormat sdf = Utils.getDateFormat();
            return sdf.format(date);
        } else if (type.equals("datetime") || type.equals("timestamp")) {
            SimpleDateFormat sdf = Utils.getDateTimeFormat();
            return sdf.format(date);
        }
        assert false : "not supported type " + type;
        return null;
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
    public static boolean mapChangesAfterUpdate(Map<String, Object> update, Map<String, Object> map, Map<String, Object> changes, List<String> indexes) {

        if (update == null || map == null) {
            return false;
        }

        Map<String, Object> todoMap = new LinkedHashMap<String, Object>();
        changes.clear();
        if (indexes != null && indexes.size() > 0) {
            for (String indexKey : indexes) {
                changes.put(indexKey, map.get(indexKey));
            }
        }
        for (Map.Entry<String, Object> entry : update.entrySet()) {
            String key = entry.getKey();
            if (!map.containsKey(key)) {
                changes.put(key, entry.getValue()); // for report purpose
                return false;
            }
            if (indexes != null && indexes.contains(key)) {
                continue;
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
        return isMapEquals(Utils.toMap(a), b);
    }

    public static boolean isMapEquals(Map<String, Object> a, String b) {
        return isMapEquals(a, Utils.toMap(b));
    }

    public static boolean isMapEquals(String a, String b) {
        return isMapEquals(Utils.toMap(a), Utils.toMap(b));
    }
}
