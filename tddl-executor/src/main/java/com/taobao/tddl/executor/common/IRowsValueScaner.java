package com.taobao.tddl.executor.common;

import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 单行结果遍历用。 大部分情况下，使用index比使用colName效率高，所以这个scaner主要作用就是 缓存一行中取到数据的index.加速访问
 * 
 * @author whisper
 */
public interface IRowsValueScaner {

    List<ISelectable> getColumnsUWantToScan();

    /**
     * 非线程安全哦。。 返回数据的rowSet;
     * 
     * @param rowSet
     * @return
     */
    Iterator<Object> rowValueIterator(IRowSet rowSet);
}
