package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.IRowsValueScaner;
import com.taobao.tddl.executor.common.RowsValueScanerImp;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMessage;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public class CursorMetaImp implements ICursorMeta {

    private CursorMetaImp(String name, List<ColumnMessage> columns, Integer indexRange){
        super();
        // this.name = name;
        this.columns = new ArrayList<ColumnMeta>();
        this.indexRange = indexRange;
        int index = 0;
        for (ColumnMessage cm : columns) {
            String colName = cm.getName();
            String tabName = name;
            addAColumn(tabName, colName, cm.getAlias(), index);
            this.columns.add(new ColumnMeta(name, cm.getName(), cm.getDataType(), cm.getAlias(), cm.getNullable()));
            index++;
        }
    }

    private CursorMetaImp(String name, List<ColumnMessage> columns, List<Integer> indexes, Integer indexRange){
        // this.name = name;
        this.columns = new ArrayList<ColumnMeta>();
        Iterator<Integer> iteratorIndex = indexes.iterator();
        for (ColumnMessage cm : columns) {
            if (!iteratorIndex.hasNext()) {
                throw new IllegalArgumentException("iterator and columns not match");
            }
            String colName = cm.getName();
            String tabName = name;
            addAColumn(tabName, colName, cm.getAlias(), iteratorIndex.next());
            this.columns.add(new ColumnMeta(name, cm.getName(), cm.getDataType(), cm.getAlias(), cm.getNullable()));
        }
        this.indexRange = indexRange;
    }

    private CursorMetaImp(List<ColumnMeta> columns, List<Integer> indexes, Integer indexRange){
        this.columns = new ArrayList<ColumnMeta>(columns);
        Iterator<Integer> iteratorIndex = indexes.iterator();
        for (ColumnMeta cm : columns) {
            if (!iteratorIndex.hasNext()) {
                throw new IllegalArgumentException("iterator and columns not match");
            }
            String colName = cm.getName();
            String tabName = cm.getTableName();
            addAColumn(tabName, colName, cm.getAlias(), iteratorIndex.next());
        }
        this.indexRange = indexRange;
    }

    private CursorMetaImp(List<ColumnMeta> columns, Integer indexRange){
        super();
        this.columns = new ArrayList<ColumnMeta>(columns);
        int index = 0;
        for (ColumnMeta cm : columns) {
            String colName = cm.getName();
            String tabName = cm.getTableName();
            addAColumn(tabName, colName, cm.getAlias(), index);
            index++;
        }
        this.indexRange = indexRange;
    }

    private CursorMetaImp(List<ColumnMeta> columns){
        super();
        this.columns = new ArrayList<ColumnMeta>(columns);
        int index = 0;
        for (ColumnMeta cm : columns) {
            String colName = cm.getName();
            String tabName = cm.getTableName();
            addAColumn(tabName, colName, cm.getAlias(), index);
            index++;
        }
        if (indexMap == null) {
            indexMap = new HashMap<String, CursorMetaImp.ColumnHolder>();
        }
        this.indexRange = index;
    }

    /**
     * 就是表名+index 因为可能出现多个表有相同列的情况。 但从速度上考虑，列必然应该做hash,否则效率太低了。表重的可能性，有，但不会很多。
     * 所以不想用map结构。
     * 
     * @author whisper
     */
    public static class ColumnHolder {

        public ColumnHolder(ColumnHolder next, String tablename, Integer index){
            super();
            this.next = next;
            this.tablename = tablename;
            this.index = index;
        }

        /**
         * 如果有同列名，不同表名，放这里， 预计不会出现很多这样的情况。
         */
        ColumnHolder next      = null;
        /**
         * 表名
         */
        String       tablename = null;
        /**
         * indexName
         */
        Integer      index     = null;

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ColumnHolder [\t");
            if (next != null) {
                builder.append("next:");
                builder.append(next);
                builder.append(", \t");
            }
            if (tablename != null) {
                builder.append("tablename:");
                builder.append(tablename);
                builder.append(",\t");
            }
            if (index != null) {
                builder.append("index:");
                builder.append(index);
            }
            builder.append("]\n");
            return builder.toString();
        }

    }

    private String                                                   name;

    private List<ColumnMeta>                                         columns;

    private Map<String/* 列名字哦。注意，不是表名，因为列名更长取，量也更大 */, ColumnHolder> indexMap = null;

    private Integer                                                  indexRange;

    private boolean                                                  isSureLogicalIndexEqualActualIndex;

    @Override
    public Integer getIndexRange() {
        return indexRange;
    }

    // @Override
    // public String getName() {
    // return name;
    // }

    @Override
    public List<ColumnMeta> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public Integer getIndex(String tableName, String columnName) {
        tableName = ExecUtils.getLogicTableName(tableName);
        ColumnHolder ch = indexMap.get(columnName);
        if (ch == null) {
            return null;
        }
        // 第一个P
        Integer index = null;
        if (tableName == null /* || ch.next == null */) {// hook tableName ==
            // null.取第一个,如果没有下一个，那么无论表名是什么都返回当前的。。因为没的选择。
            return ch.index;
        }
        index = findTableName(tableName, ch);
        if (index != null) {
            return index;
        }
        ColumnHolder nextCh = ch;
        // 其它的ColumnHolder
        while ((nextCh = nextCh.next) != null) {
            index = findTableName(tableName, nextCh);
            if (index != null) {
                return index;
            }
        }
        // throw new IllegalArgumentException("can't find Index by tableName : "
        // + tableName + " colname : " + columnName + " .index Map : "
        // + indexMap);
        return null;
    }

    private static Integer findTableName(String tableName, ColumnHolder ch) {
        if (StringUtils.equals(tableName, ch.tablename)) {
            return ch.index;
        }
        return null;
    }

    public Map<String, ColumnHolder> getIndexMap() {
        return Collections.unmodifiableMap(indexMap);
    }

    public static CursorMetaImp buildNew(List<ColumnMeta> columns) {
        return new CursorMetaImp(columns);
    }

    public static CursorMetaImp buildNew(String name, List<ColumnMessage> columns, Integer indexRange) {
        return new CursorMetaImp(name, columns, indexRange);
    }

    public static CursorMetaImp buildNew(List<ColumnMeta> columns, Integer indexRange) {
        return new CursorMetaImp(columns, indexRange);
    }

    public static CursorMetaImp buildNew(String name, List<ColumnMessage> columns, List<Integer> indexes,
                                         Integer indexRange) {
        return new CursorMetaImp(name, columns, indexes, indexRange);
    }

    public static CursorMetaImp buildNew(List<ColumnMeta> columns, List<Integer> indexes, Integer indexRange) {
        return new CursorMetaImp(columns, indexes, indexRange);
    }

    protected void addAColumn(String tableName, String colName, String colAlias, Integer index) {
        if (indexMap == null) {
            indexMap = new HashMap<String, CursorMetaImp.ColumnHolder>();
        }

        // if (aliasIndexMap == null) {
        // aliasIndexMap = new HashMap<String, CursorMetaImp.ColumnHolder>();
        // }

        tableName = ExecUtils.getLogicTableName(tableName);
        ColumnHolder ch = indexMap.get(colName);
        if (ch == null) {
            ch = new ColumnHolder(null, tableName, index);
            indexMap.put(colName, ch);

            // if (colAlias != null) aliasIndexMap.put(colAlias, ch);
            return;
        }

        boolean success = findTableAndReplaceIndexNumber(tableName, index, ch);
        if (success) {
            return;
        }
        ColumnHolder nextCh = null;
        // 其它的ColumnHolder
        while ((nextCh = ch.next) != null) {
            success = findTableAndReplaceIndexNumber(tableName, index, nextCh);
            if (success) {
                return;
            }
        }

    }

    // protected void addAColumn(Map<String, ColumnHolder> columnHolderMap,
    // String tableName, String colName, Integer index) {
    //
    // }

    /**
     * 如果能找到同表名的，就替换对应的index 如果不能找到，但链表下一个为空，则构建新的ColumnHolder 放到队尾，也算成功 其他算失败
     * 
     * @param tableName
     * @param index
     * @param ch
     * @return
     */
    private static boolean findTableAndReplaceIndexNumber(String tableName, Integer index, ColumnHolder ch) {
        boolean success = false;
        if (StringUtils.equals(tableName, ch.tablename)) {
            ch.index = index;
            success = true;
        } else if (ch.next == null) {
            ch.next = new ColumnHolder(null, tableName, index);
            success = true;
        }
        return success;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append("[");
        sb.append("cursor meta name : ").append(name).append("\t");
        List<ColumnMeta> metas = columns;
        if (metas != null) {
            for (ColumnMeta cm : metas) {
                sb.append(cm.toStringWithInden(0)).append(":");
                sb.append(getIndex(cm.getTableName(), cm.getName()));
                sb.append(" ");
            }
        }
        return sb.toString();

    }

    private static class IndexMetaIterator implements Iterator<ColMetaAndIndex> {

        Iterator<Entry<String, ColumnHolder>> entryIterator = null;
        /**
         * 临时iterator 因为可能出现同列名，不同表名的情况
         */
        ColMetaAndIndex                       current       = null;

        public IndexMetaIterator(Map<String/* 列名字 */, ColumnHolder> indexMap){
            entryIterator = indexMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (current == null) {
                return entryIterator.hasNext();
            } else {
                boolean hasNext = current.getColumnHolder().next != null;
                if (!hasNext) {
                    current = null;
                    hasNext = hasNext();
                }
                return hasNext;
            }
        }

        @Override
        public ColMetaAndIndex next() {
            if (current == null) {
                current = new ColMetaAndIndex();
                Entry<String, ColumnHolder> entry = entryIterator.next();
                if (entry == null) {
                    throw new NoSuchElementException();
                } else {
                    current.setColumnHolder(entry.getValue());
                    current.setName(entry.getKey());
                    return current;
                }
            } else {
                ColumnHolder chNext = current.getColumnHolder().next;
                if (chNext != null) {
                    current.setColumnHolder(chNext);
                    return current;
                } else {
                    current = null;
                    return next();
                }
            }
        }

        @Override
        public void remove() {
            throw new IllegalStateException();

        }

    }

    @Override
    public Iterator<ColMetaAndIndex> indexIterator() {
        return new IndexMetaIterator(indexMap);
    }

    @Override
    public IRowsValueScaner scaner(List<ISelectable> columnsYouWant) {
        RowsValueScanerImp rvs = new RowsValueScanerImp(this, columnsYouWant);
        return rvs;
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public boolean isSureLogicalIndexEqualActualIndex() {
        return this.isSureLogicalIndexEqualActualIndex;
    }

    @Override
    public void setIsSureLogicalIndexEqualActualIndex(boolean b) {
        this.isSureLogicalIndexEqualActualIndex = b;
    }

    public static ICursorMeta buildEmpty() {
        List<ColumnMeta> empty = Collections.emptyList();
        return new CursorMetaImp(empty);
    }
}
