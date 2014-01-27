package com.taobao.tddl.executor.record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.IRecord;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author mengshi.sunmengshi 2013-12-3 下午1:51:05
 * @since 5.0.0
 */
public class FixedLengthRecord extends CloneableRecord {

    private final static Logger    logger = LoggerFactory.getLogger(FixedLengthRecord.class);
    protected Object[]             values;

    protected Map<String, Integer> index;

    public FixedLengthRecord(Map<String, Integer> index, int mapSizeCache){
        this.index = index;
        this.values = new Object[index.size()];
    }

    public FixedLengthRecord(List<ColumnMeta> keys){
        this.index = new HashMap(keys.size());
        for (ColumnMeta key : keys) {
            if (!index.containsKey(key.getName())) {
                index.put(key.getName(), index.size());
            }
        }
        this.values = new Object[index.size()];
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
    public Object getIngoreTableNameUpperCased(String key) {
        return get(key);
    }

    @Override
    public CloneableRecord put(String name, String key, Object value) {
        put(key, value);
        return this;
    }

    @Override
    public IRecord put(String key, Object value) {
        Integer indexNum = index.get(key);
        if (indexNum == null) {
            throw new IllegalArgumentException("can't find key :" + key + " . current indexes is " + index);
        }
        values[indexNum] = value;
        return this;
    }

    @Override
    public Object get(String key) {
        Integer i = index.get(key);
        if (i == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("can't find key :" + key + " .index list : " + index);
            }

            return null;
        }
        return values[i];
    }

    @Override
    public IRecord putAll(Map<String, Object> all) {
        for (Entry<String, Object> e : all.entrySet()) {
            put(e.getKey(), e.getValue());
        }
        return this;
    }

    @Override
    public Object getValueByIndex(int index) {

        return values[index];
    }

    @Override
    public IRecord setValueByIndex(int index, Object val) {
        values[index] = val;
        return this;
    }

    @Override
    public IRecord addAll(List<Object> values) {
        values.addAll(values);
        return this;
    }

    @Override
    public Map<String, Integer> getColumnMap() {
        return index;
    }

    @Override
    public List<String> getColumnList() {
        List<String> ret = new ArrayList(index.size());
        ret.addAll(index.keySet());
        return ret;
    }

    @Override
    public List<Object> getValueList() {
        return Arrays.asList(values);
    }

    @Override
    public Map<String, Object> getMap() {
        Map<String, Object> ret = new HashMap(values.length);
        for (Entry<String, Integer> e : index.entrySet()) {
            ret.put(e.getKey(), values[e.getValue()]);
        }
        return ret;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compareTo(IRecord o) {
        List<Object> values1 = null;
        if (o instanceof NamedRecord) {
            values1 = ((FixedLengthRecord) ((NamedRecord) o).getRecord()).getValueList();
        } else {
            values1 = ((FixedLengthRecord) o).getValueList();
        }
        for (int k = 0; k < values1.size(); k++) {
            Object v1 = values1.get(k);
            Object v = values[k];
            int r = 0;
            if (v instanceof byte[]) {
                byte[] a = (byte[]) v;
                byte[] b = (byte[]) v1;
                int len = Math.min(a.length, b.length);
                for (int i = 0; i < len; i++) {
                    byte ai = a[i];
                    byte bi = b[i];
                    if (ai != bi) {
                        if (ai - bi != 0) {
                            r = ai - bi;
                            break;
                        }
                    }
                }
                r = a.length - b.length;
            } else {
                if (v != null && v1 != null) {
                    r = ((Comparable) v).compareTo(v1);
                } else if (v == null && v1 == null) {
                    r = 0;
                } else if (v == null) {
                    r = -1;
                } else if (v1 == null) {
                    r = 1;
                }
            }
            if (r != 0) {
                return r;
            }
        }
        return 0;
    }

    @Override
    public Object clone() {
        FixedLengthRecord fixedLengthRecord = (FixedLengthRecord) super.clone();
        if (fixedLengthRecord.values != null) {
            fixedLengthRecord.values = Arrays.copyOf(fixedLengthRecord.values, fixedLengthRecord.values.length);
        }
        return fixedLengthRecord;
    }

    @Override
    public String toString() {
        return "FixedLengthRecord{" + " values=" + Arrays.asList(values) + ", index=" + index + '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        // if (getClass() != obj.getClass()) {
        // return false;
        // }
        final FixedLengthRecord other;
        if (obj instanceof FixedLengthRecord) {
            other = (FixedLengthRecord) obj;
        } else if (obj instanceof NamedRecord) {
            NamedRecord nr = (NamedRecord) obj;
            if (nr.record != null && nr.record instanceof FixedLengthRecord) {
                other = (FixedLengthRecord) nr.record;
            } else {
                return false;
            }
        } else {
            return false;
        }

        if (!Arrays.deepEquals(this.values, other.values)) {
            return false;
        }
        if (this.index != other.index && (this.index == null || !this.index.equals(other.index))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Arrays.deepHashCode(this.values);
        hash = 29 * hash + (this.index != null ? this.index.hashCode() : 0);
        return hash;
    }

    @Override
    public DataType getType(int index) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataType getType(String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

}
