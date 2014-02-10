package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;

/**
 * n个cursor的归并排序，假设子cursor都是有序的
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:57:02
 * @since 5.0.0
 */
public class MergeSortedCursors extends SortCursor {

    private ValueMappingIRowSetConvertor valueMappingIRowSetConvertor;
    private final List<ISchematicCursor> cursors;

    /**
     * 保存每个cursor当前的值得wrapper
     */
    private List<IRowSet>                values;

    private boolean                      templateIsLeft = true;

    public MergeSortedCursors(List<ISchematicCursor> cursors, boolean duplicated) throws TddlException{
        super(cursors.get(0), null);
        this.cursors = cursors;
        this.allowDuplicated = duplicated;
        values = new ArrayList(cursors.size());
        this.orderBys = cursors.get(0).getOrderBy();
    }

    public MergeSortedCursors(ISchematicCursor cursor, boolean duplicated) throws TddlException{
        super(cursor, null);
        List<ISchematicCursor> cursors = new ArrayList(1);
        cursors.add(cursor);

        this.cursors = cursors;
        this.allowDuplicated = duplicated;
        values = new ArrayList(cursors.size());
    }

    @Override
    public void beforeFirst() throws TddlException {
        inited = false;
        values.clear();
        values = new ArrayList(cursors.size());
        for (Cursor cursor : cursors) {
            cursor.beforeFirst();
        }
    }

    boolean allowDuplicated = false;
    IRowSet current         = null;
    boolean inited          = false;

    @Override
    public void init() throws TddlException {
        if (inited) {
            return;
        }
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
    public IRowSet next() throws TddlException {
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
                    IRowSet rowToRemoveDuplicate = currentMaxOrMin;
                    while (true) {
                        rowToRemoveDuplicate = this.cursors.get(i).next();
                        if (rowToRemoveDuplicate == null) {
                            break;
                        }

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
            currentMaxOrMin = ExecUtils.fromIRowSetToArrayRowSet(currentMaxOrMin);
            IRowSet rowToRemoveDuplicate = currentMaxOrMin;
            // 选中的cursor消费一行记录，往前移动，如果有需要，还要去重
            while (true) {
                rowToRemoveDuplicate = this.cursors.get(indexOfCurrentMaxOrMin).next();
                if (rowToRemoveDuplicate == null) {
                    break;
                }

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
        if (rowSet == null) {
            return null;
        }
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
    public IRowSet current() throws TddlException {
        return current;
    }

    @Override
    public IRowSet first() throws TddlException {
        for (int i = 0; i < this.cursors.size(); i++) {
            cursors.get(i).beforeFirst();
        }
        this.values.clear();
        this.inited = false;
        this.init();

        return next();
    }

    @Override
    public void put(CloneableRecord key, CloneableRecord value) throws TddlException {
        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public Cursor getCursor() {
        return cursor;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {

        throw new UnsupportedOperationException("should not be here");
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
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
    public List<TddlException> close(List<TddlException> exs) {
        for (Cursor c : this.cursors) {
            if (c != null) {
                exs = c.close(exs);
            }
        }

        return exs;
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
