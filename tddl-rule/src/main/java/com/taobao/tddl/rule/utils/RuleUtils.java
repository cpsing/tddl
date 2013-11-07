package com.taobao.tddl.rule.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.common.model.sqljep.Comparative;
import com.taobao.tddl.rule.enumerator.Enumerator;
import com.taobao.tddl.rule.enumerator.EnumeratorImp;
import com.taobao.tddl.rule.model.AdvancedParameter;

public class RuleUtils {

    private static final Enumerator enumerator = new EnumeratorImp();

    public static void notNull(Object obj, String message) {
        if (obj == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }

    /**
     * 返回对应column的枚举值
     * 
     * @param argumentsMap
     * @param param
     * @return
     */
    public static Map<String, Set<Object>> getSamplingField(Map<String, Comparative> argumentsMap,
                                                            Set<AdvancedParameter> param) {
        // 枚举以后的columns与他们的描点之间的对应关系
        Map<String, Set<Object>> enumeratedMap = new HashMap<String, Set<Object>>(param.size());
        for (AdvancedParameter entry : param) {
            String key = entry.key;
            // 当前enumerator中指定当前规则是否需要处理交集问题。
            try {
                if (argumentsMap.containsKey(key)) {
                    Set<Object> samplingField = enumerator.getEnumeratedValue(argumentsMap.get(key),
                        entry.cumulativeTimes,
                        entry.atomicIncreateValue,
                        entry.needMergeValueInCloseInterval);
                    enumeratedMap.put(key, samplingField);
                } else {
                    // 如果sql中不存在，则进行全表扫
                    enumeratedMap.put(key, entry.enumerateRange());
                }
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException("当前列分库分表出现错误，出现错误的列名是:" + entry.key, e);
            }
        }

        return enumeratedMap;
    }

    public static <T> T cast(Object obj) {
        return (T) obj;
    }
}
