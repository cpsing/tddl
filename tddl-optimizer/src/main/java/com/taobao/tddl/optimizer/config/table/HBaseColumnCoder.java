package com.taobao.tddl.optimizer.config.table;

import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

public interface HBaseColumnCoder {

    /**
     * 把一个列转换为bytes，用于非rowKey列
     * 
     * @param type
     * @param o
     * @return
     */
    byte[] encodeToBytes(DATA_TYPE type, Object o);

    /**
     * 从bytes转换出一个列的实际值，用于非rowKey列
     * 
     * @param type
     * @param bytes
     * @return
     */
    Object decodeFromBytes(DATA_TYPE type, byte[] bytes);

    /**
     * 从string转换出一个列的实际值，用于rowKey中的列
     * 
     * @param type
     * @param str
     * @return
     */
    Object decodeFromString(DATA_TYPE type, String str);

    /**
     * 把一个列的值转换为string，用于rowKey列
     * 
     * @param type
     * @param o
     * @return
     */
    String encodeToString(DATA_TYPE type, Object o);
}
