package com.taobao.tddl.optimizer.parse.hint;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.taobao.tddl.rule.model.sqljep.Comparative;
import com.taobao.tddl.rule.model.sqljep.ComparativeMapChoicer;

/**
 * 基于tddl-rule的hint条件
 * 
 * @author jianghang 2014-1-13 下午5:09:34
 * @since 5.0.0
 */
public class RuleRouteCondition extends ExtraCmdRouteCondition implements RouteCondition, ComparativeMapChoicer {

    private Map<String, Comparative> parameters = new HashMap<String, Comparative>();

    /**
     * 添加一个默认为=的参数对
     * 
     * @param str 参数项名字
     * @param comp 参数项值，一般为基本类型或可比较类型
     */
    public void put(String key, Comparable<?> parameter) {
        if (key == null) {
            throw new IllegalArgumentException("key为null");
        }
        if (parameter instanceof Comparative) {
            parameters.put(key.toUpperCase(), (Comparative) parameter);
        } else {
            // (parameter instanceof Comparable<?>
            parameters.put(key.toUpperCase(), getComparative(Comparative.Equivalent, parameter));
        }

    }

    public Comparative getComparative(int i, Comparable<?> c) {
        return new Comparative(i, c);
    }

    public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> partnationSet) {
        Map<String, Comparative> retMap = new HashMap<String, Comparative>(parameters.size());
        for (String str : partnationSet) {
            if (str != null) {
                // 因为groovy是大小写敏感的，因此这里只是在匹配的时候转为小写，放入map中的时候仍然使用原来的大小写
                Comparative comp = parameters.get(str.toUpperCase());
                if (comp != null) {
                    retMap.put(str, comp);
                }
            }
        }
        return retMap;
    }

    public Comparative getColumnComparative(List<Object> arguments, String colName) {
        Comparative res = null;
        if (colName != null) {
            // 因为groovy是大小写敏感的，因此这里只是在匹配的时候转为小写，放入map中的时候仍然使用原来的大小写
            Comparative comp = parameters.get(colName.toUpperCase());
            if (comp != null) {
                res = comp;
            }
        }

        return res;
    }

    public Map<String, Comparative> getParameters() {
        return parameters;
    }

    public ComparativeMapChoicer getCompMapChoicer() {
        return this;
    }
}
