package com.taobao.tddl.executor.cursor.impl;

import java.util.Map;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;

/**
 * 两个rowset内容相同，但是列顺序不同，可以用此转换
 * 
 * @author mengshi.sunmengshi 2013-12-19 上午11:09:09
 * @since 5.0.0
 */
public class ValueMappingRowSet extends RowSetWrapper {

    public ValueMappingRowSet(ICursorMeta iCursorMeta, IRowSet rowSet,
                              Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping){
        super(iCursorMeta, rowSet);
        this.mapping = mapping;
    }

    final Map<Integer/* 返回列中的index位置 */, Integer/* 实际数据中的index位置 */> mapping;

    @Override
    public Object getObject(int index) {
        Integer indexReal = mapping.get(index);
        if (indexReal == null) {
            indexReal = index;
        }
        Object obj = parentRowSet.getObject(indexReal);
        return obj;
    }

    @Override
    public void setObject(int index, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getLong(int index) {
        Integer indexReal = mapping.get(index);
        if (indexReal == null) {
            indexReal = index;
        }
        Long obj = parentRowSet.getLong(indexReal);
        return obj;
    }

    @Override
    public void setLong(int index, Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(int index) {
        Integer indexReal = mapping.get(index);
        if (indexReal == null) {
            indexReal = index;
        }
        Integer obj = parentRowSet.getInteger(indexReal);
        return obj;
    }

    @Override
    public void setInteger(int index, Integer value) {
        throw new UnsupportedOperationException();
    }

}
