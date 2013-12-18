package com.taobao.tddl.executor.cursor;

import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.executor.common.IRowsValueScaner;
import com.taobao.tddl.executor.cursor.impl.ColMetaAndIndex;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * cursor中所有行数据的源信息。
 * 
 * @author Whisper
 */
public interface ICursorMeta {

    /**
     * 当前Cursor中间表的别名
     * 
     * @return
     */
    // String getName();

    List<ColumnMeta> getColumns();

    /**
     * 返回index,为null则表示未找到
     * 
     * @param tableName
     * @param columnName
     * @return
     */
    Integer getIndex(String tableName, String columnName);

    /**
     * 因为返回列可能只有三列。 但实际上数据的列数可能很多 （也就是getIndex的integer可能会大于columns的size())
     * 所以在一些做位移性操作的时候，需要依托这个值进行 尤其是在join的时候 简单来说就是，ICursorMeta可能只是个facade .
     * 在join里面实际上是用偏移量来拿数据的， 这样就需要知道数值偏移多少，能够从右IRowSet里面取数据 比如，左面去取cursor
     * column可能只有2个(因为有columnFilter)
     * 但实际上ArrayRowSet里面有10列的数据(因为原本是取出了10个记录的，又没有走一层网络，所以在数组内还保持了原有的整行数据。
     * 这样，如果只靠返回值，那么只能拿到2.用2做偏移量，是肯定不对的。 所以，需要拿真正数据的偏移量出来。所以这个数据在这时候应该返回10.
     * 
     * @return
     */
    Integer getIndexRange();

    /**
     * 按照缩进输出String的方法
     * 
     * @param inden
     * @return
     */
    String toStringWithInden(int inden);

    Iterator<ColMetaAndIndex> indexIterator();

    IRowsValueScaner scaner(List<ISelectable> columnsYouWant);

    /**
     * 是否确定UResultSet中的index和resultCusror中的index相等?
     * 这个属性的主要作用在于提升ResultSet在进行单值查询时候的性能。 为true表示一定相等，为false未必不相等
     * 
     * @return
     */
    boolean isSureLogicalIndexEqualActualIndex();

    void setIsSureLogicalIndexEqualActualIndex(boolean b);
}
