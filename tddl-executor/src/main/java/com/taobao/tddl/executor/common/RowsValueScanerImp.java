package com.taobao.tddl.executor.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 根据给定的字段遍历, ps. 期望的字段列表不一定是cursor meta中的返回顺序，需要做index映射
 * 
 * @author mengshi.sunmengshi 2013-12-3 下午1:53:33
 * @since 5.0.0
 */
public class RowsValueScanerImp implements IRowsValueScaner {

    private final ICursorMeta       cursorMeta;
    private final List<ISelectable> columnsYouWant;
    private final List<Integer>     indexList;

    public RowsValueScanerImp(ICursorMeta cursorMeta, List<ISelectable> left_columns){
        super();
        this.cursorMeta = cursorMeta;
        this.columnsYouWant = left_columns;
        indexList = new ArrayList<Integer>(left_columns.size());
        for (ISelectable icol : left_columns) {
            Integer index = this.cursorMeta.getIndex(icol.getTableName(), icol.getColumnName());
            if (index == null && icol.getAlias() != null) {
                index = this.cursorMeta.getIndex(icol.getTableName(), icol.getAlias());
            }

            indexList.add(index);
        }

    }

    @Override
    public List<ISelectable> getColumnsUWantToScan() {
        return columnsYouWant;
    }

    public static class RowValueIteratorImp implements Iterator<Object> {

        private List<Integer> indexList;
        private int           index;
        private int           indexListSize;
        private final IRowSet iRowSet;

        public RowValueIteratorImp(List<Integer> indexList, IRowSet iRowSet){
            super();
            this.indexList = indexList;
            indexListSize = indexList.size();
            this.iRowSet = iRowSet;
        }

        @Override
        public boolean hasNext() {
            return index < indexListSize;
        }

        @Override
        public Object next() {
            Integer iRowSetindex = indexList.get(index);
            Object comp = iRowSet.getObject(iRowSetindex);
            index++;
            return comp;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();

        }

    }

    public Iterator<Object> rowValueIterator(IRowSet rowSet) {
        return new RowValueIteratorImp(indexList, rowSet);
    }

}
