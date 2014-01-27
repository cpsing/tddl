//package com.taobao.tddl.executor.record;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import com.taobao.tddl.executor.common.IRecord;
//import com.taobao.tddl.optimizer.config.table.ColumnMeta;
//
///**
// * @author mengshi.sunmengshi 2013-12-3 下午1:51:18
// * @since 5.0.0
// */
//public class FixedLengthBaseRecord extends NamedRecord {
//
//    public FixedLengthBaseRecord(List<ColumnMeta> keys){
//        this(keys, "");
//    }
//
//    public FixedLengthBaseRecord(List<ColumnMeta> keys, String tableName){
//        this.index = new HashMap<String, Integer>(keys.size());
//        for (ColumnMeta key : keys) {
//            index.put(key.getName(), index.size());
//        }
//        this.values = new Object[index.size()];
//        this.tableName = tableName;
//        this.name = tableName;
//    }
//
//    public FixedLengthBaseRecord(Map<String, Integer> index, int mapSizeCache){
//        this(index, mapSizeCache, "");
//    }
//
//    public FixedLengthBaseRecord(Map<String, Integer> index, int mapSizeCache, String tableName){
//        this.index = index;
//        this.values = new Object[mapSizeCache];
//        this.tableName = tableName;
//        this.name = tableName;
//    }
//
//    String               tableName;
//    String               indexName;
//    Object[]             values;
//    Map<String, Integer> index;
//
//    public String getTableName() {
//        return tableName;
//    }
//
//    public void setTableName(String tableName) {
//        this.tableName = tableName;
//    }
//
//    @Override
//    public Object get(String name, String key) {
//        return getIngoreTableName(key);
//    }
//
//    @Override
//    public Object getIngoreTableName(String key) {
//        Integer i = index.get(key);
//        if (i == null) {
//            return null;
//        }
//        return values[(index.get(key))];
//    }
//
//    @Override
//    public CloneableRecord put(String name, String key, Object value) {
//        put(key, value);
//        return this;
//    }
//
//    @Override
//    public IRecord put(String key, Object value) {
//        values[index.get(key)] = value;
//        return this;
//    }
//
//    @Override
//    public Object get(String key) {
//        Integer i = index.get(key);
//        if (i == null) {
//            return null;
//        }
//        return values[index.get(key)];
//    }
//
//    @Override
//    public IRecord putAll(Map<String, Object> all) {
//        for (Entry<String, Object> e : all.entrySet()) {
//            put(e.getKey(), e.getValue());
//        }
//        return this;
//    }
//
//    @Override
//    public Object getValueByIndex(int index) {
//        return values[index];
//    }
//
//    @Override
//    public IRecord setValueByIndex(int index, Object val) {
//        values[index] = val;
//        return this;
//    }
//
//    @Override
//    public IRecord addAll(List<Object> values) {
//        values.addAll(values);
//        return this;
//    }
//
//    @Override
//    public Map<String, Integer> getColumnMap() {
//        return index;
//    }
//
//    @Override
//    public List<String> getColumnList() {
//        List<String> ret = new ArrayList(index.size());
//        ret.addAll(index.keySet());
//        return ret;
//    }
//
//    @Override
//    public List<Object> getValueList() {
//        return Arrays.asList(values);
//    }
//
//    @Override
//    public Map<String, Object> getMap() {
//        Map<String, Object> ret = new HashMap(values.length);
//        for (Entry<String, Integer> e : index.entrySet()) {
//            ret.put(e.getKey(), values[e.getValue()]);
//        }
//        return ret;
//    }
//
//    @Override
//    public int compareTo(IRecord o) {
//        List<Object> values1 = null;
//        if (o instanceof NamedRecord) {
//            values1 = ((FixedLengthRecord) ((NamedRecord) o).getRecord()).getValueList();
//        } else {
//            values1 = ((FixedLengthRecord) o).getValueList();
//        }
//        for (int k = 0; k < values1.size(); k++) {
//            Object v1 = values1.get(k);
//            Object v = values[k];
//            int r = 0;
//            if (v instanceof byte[]) {
//                byte[] a = (byte[]) v;
//                byte[] b = (byte[]) v1;
//                int len = Math.min(a.length, b.length);
//                for (int i = 0; i < len; i++) {
//                    byte ai = a[i];
//                    byte bi = b[i];
//                    if (ai != bi) {
//                        if (ai - bi != 0) {
//                            r = ai - bi;
//                            break;
//                        }
//                    }
//                }
//                r = a.length - b.length;
//            } else {
//                if (v != null && v1 != null) {
//                    r = ((Comparable) v).compareTo((Comparable) v1);
//                } else if (v == null && v1 == null) {
//                    r = 0;
//                } else if (v == null) {
//                    r = -1;
//                } else if (v1 == null) {
//                    r = 1;
//                }
//            }
//            if (r != 0) {
//                return r;
//            }
//        }
//        return 0;
//    }
//
//    @Override
//    public Object clone() {
//        return super.clone();
//    }
//
//    @Override
//    public String toString() {
//        return "FixedLengthRecord{" + " values=" + Arrays.asList(values) + ", index=" + index + '}';
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        if (obj == null) {
//            return false;
//        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
//        final FixedLengthRecord other = (FixedLengthRecord) obj;
//        if (!Arrays.deepEquals(this.values, other.values)) {
//            return false;
//        }
//        if (this.index != other.index && (this.index == null || !this.index.equals(other.index))) {
//            return false;
//        }
//        return true;
//    }
//
//    @Override
//    public int hashCode() {
//        int hash = 7;
//        hash = 29 * hash + Arrays.deepHashCode(this.values);
//        hash = 29 * hash + (this.index != null ? this.index.hashCode() : 0);
//        return hash;
//    }
//
// }
