package com.taobao.tddl.executor.common;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author mengshi.sunmengshi 2013-12-2 下午4:22:43
 * @since 5.0.0
 */
public interface IRecord extends Comparable<IRecord> {

    /**
     * 塞入一个值
     * 
     * @param key
     * @param value
     * @return
     */
    IRecord put(String key, Object value);

    /**
     * 获取一个值
     * 
     * @param key
     * @return
     */
    Object get(String key);

    /**
     * 塞入所有值
     * 
     * @param all
     * @return
     */
    IRecord putAll(Map<String, Object> all);

    /**
     * 使用index 获取值，理论上来说，效率最高，因为使用下标
     * 
     * @param index
     * @return
     */
    Object getValueByIndex(int index);

    /**
     * 设置某个值,理论上来说效率最高
     * 
     * @param index
     * @param val
     * @return
     */
    IRecord setValueByIndex(int index, Object val);

    IRecord addAll(List<Object> values);

    // IRecord putAllColumnMap(Map<String/*column name*/, Integer/*column
    // index*/> map);
    /**
     * 获取columnMap
     * 
     * @return
     */
    Map<String/* column name */, Integer/* column index */> getColumnMap();

    /**
     * 遍历ColumnList
     * 
     * @return
     */
    List<String> getColumnList();

    List<Object> getValueList();

    Map<String, Object> getMap();

    DataType getType(int index);

    DataType getType(String columnName);

}
