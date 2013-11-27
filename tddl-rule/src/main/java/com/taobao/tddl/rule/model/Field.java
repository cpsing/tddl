package com.taobao.tddl.rule.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * <pre>
 * 存放列名->sourceKey的映射，比如支持id in (xx)时，根据param计算后得出了目标库地址，可将该记录的sourceKey的发送到目标库上进行执行.
 * </pre>
 * 
 * @author shenxun
 */
public class Field {

    public static final Field                                EMPTY_FIELD = new Field(0);
    private Map<String/* 列名 */, Set<Object>/* 得到该结果的描点值名 */> sourceKeys;

    public Field(int capacity){
        sourceKeys = new HashMap<String, Set<Object>>(capacity);
    }

    public boolean equals(Object obj, Map<String, String> alias) {
        // 用于比较两个field是否相等。field包含多个列，那么多列内的每一个值都应该能找到对应的值才算相等。
        if (!(obj instanceof Field)) {
            return false;
        }
        Map<String, Set<Object>> target = ((Field) obj).sourceKeys;
        for (Entry<String, Set<Object>> entry : sourceKeys.entrySet()) {
            String srcKey = entry.getKey();
            if (alias.containsKey(srcKey)) {
                srcKey = alias.get(srcKey);
            }
            Set<Object> targetValueSet = target.get(srcKey);
            Set<Object> sourceValueSet = entry.getValue();
            for (Object srcValue : sourceValueSet) {
                boolean eq = false;
                for (Object tarValue : targetValueSet) {
                    if (tarValue.equals(srcValue)) {
                        eq = true;
                    }
                }
                if (!eq) {
                    return false;
                }
            }
        }
        return true;
    }

    public Map<String, Set<Object>> getSourceKeys() {
        return sourceKeys;
    }

    public void setSourceKeys(Map<String, Set<Object>> sourceKeys) {
        this.sourceKeys = sourceKeys;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
