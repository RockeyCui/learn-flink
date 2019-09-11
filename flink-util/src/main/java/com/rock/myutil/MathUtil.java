package com.rock.myutil;

import java.math.BigDecimal;

/**
 * @author RockeyCui
 */
public class MathUtil {

    public static Long getLongVal(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Long.valueOf((String) obj);
        } else if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Integer) {
            return Long.valueOf(obj.toString());
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Long.");
    }

    public static Integer getIntegerVal(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).intValue();
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Integer.");
    }

    public static Boolean getBoolean(Object obj, boolean defaultVal) {
        if (obj == null) {
            return defaultVal;
        }
        return getBoolean(obj);
    }

    public static Boolean getBoolean(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Boolean.");
    }

    public static String getString(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return (String) obj;
        } else {
            return obj.toString();
        }
    }
}
