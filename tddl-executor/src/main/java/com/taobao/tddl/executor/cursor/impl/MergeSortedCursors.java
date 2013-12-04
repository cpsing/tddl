package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.IORCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * n个cursor的归并排序，假设子cursor都是有序的
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:57:02
 * @since 5.1.0
 */
public class MergeSortedCursors extends SortCursor implements IORCursor {

    private ValueMappingIRowSetConvertor valueMappingIRowSetConvertor;
    private List<ISchematicCursor>       cursors;
    private String                       tableAlias;

    /**
     * 保存每个cursor当前的值得wrapper
     */
    private List<IRowSet>                values;
    //
    // public MergeSortedCursors(List<ISchematicCursor> cursors,String
    // tableAlias)
    // throws FetchException {
    // super(cursors.get(0), cursors.get(0) == null ? null : cursors.get(0)
    // .getOrderBy());
    // this.cursors = cursors;
    // }

    private boolean                      templateIsLeft = true;

    public MergeSortedCursors(List<ISchematicCursor> cursors, String tableAlias, boolean duplicated)
                                                                                                    throws TddlException{
        super(cursors.get(0), null);
        this.cursors = cursors;
        this.allowDuplicated = duplicated;
        values = new ArrayList(cursors.size());
        this.tableAlias = tableAlias;
        buildAndSetOrderBy(cursors.get(0), tableAlias);
    }

    public MergeSortedCursors(ISchematicCursor cursor, String tableAlias, boolean duplicated) throws TddlException{
        super(cursor, null);
        List<ISchematicCursor> cursors = new ArrayList(1);
        cursors.add(cursor);

        this.cursors = cursors;
        this.allowDuplicated = duplicated;
        values = new ArrayList(cursors.size());

        buildAndSetOrderBy(cursor, tableAlias);

    }

    @Override
    public void beforeFirst() throws Exception {
        inited = false;
        values.clear();
        values = new ArrayList(cursors.size());
        for (Cursor cursor : cursors)
            cursor.beforeFirst();
    }

    /**
     * Whisper . 这里加了一个方法，复制order by 然后把column的名字改成了tableAlias。
     * 否则无法从结果集中拿到正确的名字。。因为结果集的CursorMeta里面实际上存储的是tableAlias.column(or
     * columnAlias) 所以，在需要用到合并排序的时候，只能也使用类似的方法来完成了。
     * 
     * @param cursor
     * @param tableAlias
     */
    private void buildAndSetOrderBy(ISchematicCursor cursor, String tableAlias) {
        if (cursor != null) {
            List<IOrderBy> orderBy = cursor.getOrderBy();
            List<IOrderBy> orderbyAfterCopy = ExecUtils.copyOrderBys(orderBy);
            if (tableAlias != null && !tableAlias.isEmpty()) {
                for (IOrderBy orderby : orderbyAfterCopy) {
                    orderby.getColumn().setTableName(tableAlias);
                }
            }
            this.tableAlias = tableAlias;

            setOrderBy(orderbyAfterCopy);
        }
    }

    boolean allowDuplicated = false;
    IRowSet current         = null;
    boolean inited          = false;

    public void init() throws Exception {
        if (inited) return;
        inited = true;

        for (int i = 0; i < cursors.size(); i++) {
            IRowSet row = cursors.get(i).next();

            row = convertToIRowSet(row, i == 0);

            values.add(row);
        }
    }

    IRowSet currentMaxOrMin = null;

    /**
     * values中存着每个cursor的当前值 每次调用next，从values中找出最小的值，并且将对应的cursor前移 将新值存到values中
     * 如果去重，需要将重复的值略过
     */
    @Override
    public IRowSet next() throws Exception {
        init();

        int indexOfCurrentMaxOrMin = 0;
        currentMaxOrMin = null;
        for (int i = 0; i < this.values.size(); i++) {
            IRowSet row = this.values.get(i);

            if (row == null) {
                continue;
            }

            if (currentMaxOrMin == null) {
                currentMaxOrMin = row;
                indexOfCurrentMaxOrMin = i;
                continue;
            }

            super.initComparator(orderBys, row.getParentCursorMeta());

            int n = kvPairComparator.compare(row, currentMaxOrMin);

            if (n < 0) {
                currentMaxOrMin = row;
                indexOfCurrentMaxOrMin = i;
            } else if (n == 0) {
                if (!this.allowDuplicated) {
                    // 去重
                    // 把其他cursor中重复的记录消耗光
                    IRowSet rowToRemoveDuplicate = null;
                    while (true) {
                        rowToRemoveDuplicate = this.cursors.get(i).next();

                        if (rowToRemoveDuplicate == null) break;

                        rowToRemoveDuplicate = convertToIRowSet(rowToRemoveDuplicate, i == 0);

                        if (kvPairComparator.compare(rowToRemoveDuplicate, currentMaxOrMin) != 0) {
                            break;
                        }
                    }
                    this.values.set(i, rowToRemoveDuplicate);
                }
            }
        }

        if (currentMaxOrMin != null) {
            IRowSet rowToRemoveDuplicate = null;
            currentMaxOrMin = ExecUtils.fromIRowSetToArrayRowSet(currentMaxOrMin);

            // 选中的cursor消费一行记录，往前移动，如果有需要，还要去重
            while (true) {
                rowToRemoveDuplicate = this.cursors.get(indexOfCurrentMaxOrMin).next();

                if (rowToRemoveDuplicate == null) break;

                rowToRemoveDuplicate = convertToIRowSet(rowToRemoveDuplicate, indexOfCurrentMaxOrMin == 0);

                if (this.allowDuplicated) {
                    break;
                }
                super.initComparator(orderBys, rowToRemoveDuplicate.getParentCursorMeta());
                if (kvPairComparator.compare(rowToRemoveDuplicate, currentMaxOrMin) != 0) {
                    break;
                }
            }
            this.values.set(indexOfCurrentMaxOrMin, rowToRemoveDuplicate);

        }

        setCurrent(currentMaxOrMin);
        return currentMaxOrMin;

    }

    private IRowSet convertToIRowSet(IRowSet rowSet, boolean left) {

        if (rowSet == null) return null;
        if (valueMappingIRowSetConvertor == null) {// 认为是初始化
            valueMappingIRowSetConvertor = new ValueMappingIRowSetConvertor();
            valueMappingIRowSetConvertor.wrapValueMappingIRowSetIfNeed(rowSet);
            valueMappingIRowSetConvertor.reset();
            templateIsLeft = left;
        }
        if (templateIsLeft == left) {
            // template is left and current is left
            // template is right and current is right
            return rowSet;
        } else {
            // template is left but current is right
            // or template is right but current is left
            return valueMappingIRowSetConvertor.wrapValueMappingIRowSetIfNeed(rowSet);
        }

    }

    private void setCurrent(IRowSet current) {
        this.current = current;
    }

    @Override
    public IRowSet first() throws Exception {
        for (int i = 0; i < this.cursors.size(); i++) {
            cursors.get(i).beforeFirst();
        }
        this.values.clear();
        this.inited = false;
        this.init();

        return next();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws Exception {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public Cursor getCursor() {
        return cursor;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws Exception {

        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws Exception {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public String toStringWithInden(int inden) {

        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "MergeSortedCursor ");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);

        for (Cursor sub : this.cursors) {
            sb.append(sub.toStringWithInden(inden + 1));
        }

        return sb.toString();

    }

    @Override
    public List<Exception> close(List<Exception> exs) {

        for (Cursor c : this.cursors) {
            if (c != null) exs = c.close(exs);
        }

        return exs;

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
