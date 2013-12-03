package com.taobao.tddl.executor.cursor.impl;

import java.util.Map;

import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.IRowSetWrapper;

public class ValueMappingRowSet extends IRowSetWrapper {

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
