package com.rock.flink19.lookup.function;

import java.util.Arrays;
import java.util.List;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public class FieldUtil {

    public static int[] getFieldIndexes(Object[] allFields, Object[] fields) {
        int[] indexes = new int[fields.length];
        List<Object> allFieldsList = Arrays.asList(allFields);
        List<Object> fieldsList = Arrays.asList(fields);
        for (int i = 0; i < fieldsList.size(); i++) {
            indexes[i] = allFieldsList.indexOf(fieldsList.get(i));
        }
        return indexes;
    }

    public static String[] getRedisKeys(String keyPre, String[] otherFieldNames) {
        String[] redisKeys = new String[otherFieldNames.length];
        for (int i = 0; i < otherFieldNames.length; i++) {
            String redisKey = keyPre + ":" + otherFieldNames[i];
            redisKeys[i] = redisKey;
        }
        return redisKeys;
    }


}
