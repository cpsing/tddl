package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.jdbc.ParameterContext;

/**
 * 一条需要执行的sql
 * 
 * @author whisper
 */
public class Sql {

    /**
     * 执行的sql
     */
    private String                         sql;
    /**
     * 绑定变量
     */
    private Map<Integer, ParameterContext> param;
    private Object                         ds;

    public void clear() {
        param.clear();
    }

    public boolean containsKey(Object key) {
        return param.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return param.containsValue(value);
    }

    public Set<Entry<Integer, ParameterContext>> entrySet() {
        return param.entrySet();
    }

    public boolean equals(Object o) {
        return param.equals(o);
    }

    public ParameterContext get(Object key) {
        return param.get(key);
    }

    public int hashCode() {
        return param.hashCode();
    }

    public boolean isEmpty() {
        return param.isEmpty();
    }

    public Set<Integer> keySet() {
        return param.keySet();
    }

    public ParameterContext put(Integer key, ParameterContext value) {
        return param.put(key, value);
    }

    public void putAll(Map<? extends Integer, ? extends ParameterContext> m) {
        param.putAll(m);
    }

    public ParameterContext remove(Object key) {
        return param.remove(key);
    }

    public int size() {
        return param.size();
    }

    public Collection<ParameterContext> values() {
        return param.values();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<Integer, ParameterContext> getParam() {
        return param;
    }

    public void setParam(Map<Integer, ParameterContext> param) {
        this.param = param;
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder = new StringBuilder();
        if (sql != null) {
            builder.append(sql);
        }
        // 非空判断
        if (param != null && !param.isEmpty()) builder.append(toString(param.entrySet(), maxLen));
        builder.append("  " + this.getDataSource());
        builder.append("\n");
        return builder.toString();
    }

    private String toString(Collection<?> collection, int maxLen) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        int i = 0;
        for (Iterator<?> iterator = collection.iterator(); iterator.hasNext() && i < maxLen; i++) {
            if (i > 0) builder.append(", ");
            builder.append(iterator.next());
        }
        builder.append("]");
        return builder.toString();
    }

    public void setDataSoruce(Object ds) {

        this.ds = ds;

    }

    public Object getDataSource() {
        return this.ds;
    }

}
