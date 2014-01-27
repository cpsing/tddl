package com.taobao.tddl.optimizer.config.table;

import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @author mengshi.sunmengshi 2013-11-21 下午5:19:16
 * @since 5.0.0
 */
public interface HBaseColumnCoder {

    /**
     * 把一个列转换为bytes，用于非rowKey列
     * 
     * @param type
     * @param o
     * @return
     */
    byte[] encodeToBytes(DataType type, Object o);

    /**
     * 从bytes转换出一个列的实际值，用于非rowKey列
     * 
     * @param type
     * @param bytes
     * @return
     */
    Object decodeFromBytes(DataType type, byte[] bytes);

    /**
     * 从string转换出一个列的实际值，用于rowKey中的列
     * 
     * @param type
     * @param str
     * @return
     */
    Object decodeFromString(DataType type, String str);

    /**
     * 把一个列的值转换为string，用于rowKey列
     * 
     * @param type
     * @param o
     * @return
     */
    String encodeToString(DataType type, Object o);
}
