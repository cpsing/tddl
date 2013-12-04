package com.taobao.tddl.executor.record;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.executor.cursor.IRecord;

public class MapRecord extends CloneableRecord {

    Map<String, Object> kv = new HashMap<String, Object>();

    @Override
    public IRecord put(String key, Object value) {
        kv.put(key, value);
        return this;
    }

    @Override
    public Object getIngoreTableNameUpperCased(String key) {
        return get(key);
    }

    @Override
    public Object get(String key) {
        return kv.get(key);
    }

    @Override
    public IRecord putAll(Map<String, Object> all) {
        kv.putAll(all);
        return this;
    }

    @Override
    public Object getValueByIndex(int index) {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public IRecord setValueByIndex(int index, Object val) {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public IRecord addAll(List<Object> values) {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Map<String, Integer> getColumnMap() {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public List<String> getColumnList() {
        List<String> cols = new LinkedList<String>();
        for (Entry<String, Object> entry : kv.entrySet()) {
            cols.add(entry.getKey());
        }
        return cols;
    }

    @Override
    public List<Object> getValueList() {
        return Arrays.asList(this.kv.values().toArray());
    }

    @Override
    public Map<String, Object> getMap() {
        return kv;
    }

    @Override
    public int compareTo(IRecord o) {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public Object get(String name, String key) {
        return getIngoreTableName(key);
    }

    @Override
    public Object getIngoreTableName(String key) {
        return get(key);
    }

    @Override
    public CloneableRecord put(String name, String key, Object value) {
        put(key, value);
        return this;
    }

}
