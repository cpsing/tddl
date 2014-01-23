package com.taobao.tddl.executor.record;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.executor.common.IRecord;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class NamedRecord extends CloneableRecord {

    String          name;
    CloneableRecord record;

    public NamedRecord(){
    }

    public NamedRecord(String name, CloneableRecord record){
        this.name = name;
        this.record = record;
    }

    @Override
    public boolean equals(Object obj) {
        if (record != null) {
            return record.equals(obj);
        } else {
            return super.equals(obj);
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public IRecord getRecord() {
        return record;
    }

    @Override
    public Object clone() {
        NamedRecord nameR = (NamedRecord) super.clone();
        if (nameR.record != null) {
            nameR.record = (CloneableRecord) nameR.record.clone();
        }

        return nameR;
    }

    @Override
    public IRecord setValueByIndex(int index, Object val) {
        return record.setValueByIndex(index, val);
    }

    @Override
    public IRecord putAll(Map<String, Object> all) {
        return record.putAll(all);
    }

    @Override
    public IRecord put(String key, Object value) {
        // key = StringUtil.toUpperCase(key);
        return record.put(key, value);
    }

    @Override
    public List<Object> getValueList() {
        return record.getValueList();
    }

    @Override
    public Object getValueByIndex(int index) {
        return record.getValueByIndex(index);
    }

    @Override
    public Map<String, Object> getMap() {
        return record.getMap();
    }

    @Override
    public Map<String, Integer> getColumnMap() {
        return record.getColumnMap();
    }

    @Override
    public List<String> getColumnList() {
        return record.getColumnList();
    }

    @Override
    public Object get(String key) {
        return record.get(key);
    }

    @Override
    public IRecord addAll(List<Object> values) {
        return record.addAll(values);
    }

    @Override
    public int compareTo(IRecord o) {
        return record.compareTo(((NamedRecord) o).record);
    }

    @Override
    public Object get(String name, String key) {
        return get(key);
    }

    @Override
    public String toString() {
        return "NamedRecord{" + "name=" + name + ", record=" + record + '}';
    }

    @Override
    public CloneableRecord put(String name, String key, Object value) {
        return (CloneableRecord) put(key, value);
    }

    @Override
    public Object getIngoreTableName(String key) {
        return get(key);
    }

    @Override
    public Object getIngoreTableNameUpperCased(String key) {
        return get(key);
    }

    @Override
    public int hashCode() {
        if (record != null) {
            return record.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public DataType getType(int index) {
        return record.getType(index);
    }

    @Override
    public DataType getType(String columnName) {
        return record.getType(columnName);
    }

}
