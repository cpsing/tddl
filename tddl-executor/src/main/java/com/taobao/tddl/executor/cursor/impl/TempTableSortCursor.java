package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.common.TransactionConfig.Isolation;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ITempTableSortCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;
import com.taobao.tddl.executor.spi.ICursorFactory;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.IndexType;
import com.taobao.tddl.optimizer.config.table.Relationship;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 用于临时表排序，需要依赖bdb
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:01:15
 * @since 5.1.0
 */
public class TempTableSortCursor extends SortCursor implements ITempTableSortCursor {

    private final static Logger logger           = LoggerFactory.getLogger(TempTableSortCursor.class);

    protected static AtomicLong seed             = new AtomicLong(0);
    protected long              sizeProtection   = 100000;
    protected ICursorFactory    cursorFactory;
    boolean                     sortedDuplicates;

    private static final String identity         = "__identity__".toUpperCase();

    private boolean             inited           = false;

    private final IRepository   repo;
    protected ISchematicCursor  tempTargetCursor = null;
    ITable                      targetTable      = null;

    protected ICursorMeta       returnMeta       = null;
    private final long          requestID;
    ExecutionContext            executionContext = null;

    /**
     * @param cursorFactory
     * @param repo
     * @param cursor
     * @param orderBys 按照何列排序
     * @param sortedDuplicates 是否允许重复
     * @throws FetchException
     * @throws TddlException
     */
    public TempTableSortCursor(ICursorFactory cursorFactory, IRepository repo, ISchematicCursor cursor,
                               List<IOrderBy> orderBys, boolean sortedDuplicates, long requestID,
                               ExecutionContext executionContext) throws TddlException, TddlException{
        super(cursor, orderBys);
        this.sortedDuplicates = sortedDuplicates;
        setCursorFactory(cursorFactory);
        tempTargetCursor = cursor;
        this.repo = repo;
        this.requestID = requestID;
        this.executionContext = executionContext;
    }

    private void initTTSC() throws TddlException {
        if (!inited) {
            prepare(repo, tempTargetCursor, orderBys);
            inited = true;
        }

    }

    protected ISchematicCursor prepare(IRepository repo, ISchematicCursor cursor, List<IOrderBy> orderBys)
                                                                                                          throws TddlException {

        List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
        List<ColumnMeta> values = new ArrayList<ColumnMeta>();
        // 用来生成CursorMeta的 列
        List<ColumnMeta> metaColumns = new ArrayList<ColumnMeta>();
        List<ColumnMeta> metaValues = new ArrayList<ColumnMeta>();
        // 遍历cursor的keyColumn，如果某个列是order by的条件，那么放到temp
        // cursor的key里面，否则放到value里面
        IRowSet rowSet = cursor.next();
        if (rowSet != null) {
            // 希望通过kv中的列来构造meta data，因为底层讲avg(pk)，解释成了count，和 sum
            buildColumnMeta(cursor, orderBys, columns, values, rowSet, metaValues, metaColumns);
        } else {
            // 如果没值返回空 ，什么都不做
            return null;
        }
        String oldTableName = rowSet.getParentCursorMeta().getColumns().iterator().next().getTableName();

        String tableName;
        synchronized (this.getClass()) {
            seed.addAndGet(1);
            // 因为tempTable里面的表名不是外部所用的表名，而是使用tmp作为表名，所以需要先变成外部表名. 这里非常hack..
            // 因为实际做cursor的表名匹配的时候，使用的是截取法。。取第一个"."之前的作为匹配标志。
            tableName = oldTableName + ".tmp." + System.currentTimeMillis() + "." + seed + "requestID." + requestID;

            if (logger.isDebugEnabled()) {
                logger.warn("tempTableName:\n" + tableName);
            }
        }

        IndexMeta primary_meta = new IndexMeta(tableName,
            columns,
            values,
            IndexType.BTREE,
            Relationship.ONE_TO_ONE,
            true,
            true,
            null);

        TableMeta tmpSchema = new TableMeta(tableName, new ArrayList(), primary_meta, null);

        tmpSchema.setTmp(true);
        tmpSchema.setSortedDuplicates(sortedDuplicates);
        // 增加临时表的判定
        targetTable = repo.getTempTable(tmpSchema);
        CloneableRecord key = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(columns).newEmptyRecord();
        CloneableRecord value = null;
        if (values != null && values.size() != 0) {
            value = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(values).newEmptyRecord();
        }
        int i = 0;
        long size = 0;
        boolean protection = false;
        // 建立临时表，将老数据插入新表中。

        if (rowSet != null) {
            do {
                size++;
                if (size > sizeProtection) {
                    protection = true;
                    break;
                }
                for (ColumnMeta column : columns) {
                    String colName = column.getName();
                    if (colName.contains(".")) {
                        String[] sp = TStringUtil.split(colName, ".");
                        colName = sp[sp.length - 1];
                    }
                    /**
                     * 在临时表的时候，来自不同2个表的相同的列，比如 a.id和b.id
                     * 会在这里被合并，导致后面取值有问题。现在准备将临时表中的columnName改成
                     * tableName.columnName的形式。顺序不变可以将后面的取值完成 有点hack..
                     */
                    Object o = ExecUtils.getValueByTableAndName(rowSet, column.getTableName(), colName);
                    key.put(column.getName(), o);
                }
                if (values != null && values.size() != 0) {
                    for (ColumnMeta valueColumn : values) {
                        String colName = valueColumn.getName();
                        if ("__IDENTITY__".equals(colName)) {
                            continue;
                        }
                        if (colName.contains(".")) {
                            // String[] sp = StringUtil.split(colName, ".");
                            // colName = sp[sp.length-1];
                            int m = colName.indexOf(".");
                            colName = colName.substring(m + 1, colName.length());
                        }
                        /**
                         * 在临时表的时候，来自不同2个表的相同的列，比如 a.id和b.id
                         * 会在这里被合并，导致后面取值有问题。现在准备将临时表中的columnName改成
                         * tableName.columnName的形式。顺序不变可以将后面的取值完成 有点hack..
                         */
                        Object o = ExecUtils.getValueByTableAndName(rowSet, valueColumn.getTableName(), colName);
                        value.put(valueColumn.getName(), o);
                    }
                }
                // value内加入唯一索引key。
                if (sortedDuplicates) {
                    value.put(identity, i++);
                }
                // System.out.println("TempTableSortCursor: "+key+"  "+value);
                targetTable.put(this.executionContext, key, value, primary_meta, tableName);

            } while ((rowSet = cursor.next()) != null);
        }

        ExecutionContext tmpContext = new ExecutionContext();
        tmpContext.setIsolation(Isolation.READ_UNCOMMITTED);
        ISchematicCursor ret = targetTable.getCursor(tmpContext, primary_meta, tableName);

        // 去除唯一标志
        List<ColumnMeta> retColumns = new ArrayList<ColumnMeta>();
        // 将唯一标志，从返回数据内排除
        retColumns.addAll(metaColumns);
        for (ColumnMeta cm : metaValues) {
            if (!identity.equals(cm.getName())) {
                retColumns.add(cm);
            }
        }

        if (!orderBys.get(0).getDirection()) {
            ret = cursorFactory.reverseOrderCursor(executionContext, ret);
        }

        List<TddlException> exs = new ArrayList();
        exs = cursor.close(exs);
        if (!exs.isEmpty()) {
            throw GeneralUtil.mergeException(exs);
        }
        if (protection) {
            exs = this.close(exs);
            if (!exs.isEmpty()) {
                throw GeneralUtil.mergeException(exs);
            }
            throw new IllegalStateException("temp table size protection , check your sql or enlarge the limination size . ");
        }

        this.cursor = ret;
        returnMeta = CursorMetaImp.buildNew(retColumns);
        return ret;
    }

    private void buildColumnMeta(ISchematicCursor cursor, List<IOrderBy> orderBys, List<ColumnMeta> columns,
                                 List<ColumnMeta> values, IRowSet kv, List<ColumnMeta> metaValues,
                                 List<ColumnMeta> metaColumns) {
        ICursorMeta iCursorMeta = kv.getParentCursorMeta();
        List<ColumnMeta> columnMeta = iCursorMeta.getColumns();
        Set<IOrderBy> hashOrderBys = new HashSet<IOrderBy>();
        for (ColumnMeta cm : columnMeta) {
            if (findOrderByInKey(orderBys, columns, cm, hashOrderBys)) {
                if (!metaColumns.contains(cm)) metaColumns.add(cm);
                continue;
            } else {
                // 列名与order by not match ,放到value里
                ColumnMeta cm2 = new ColumnMeta(cm.getTableName(),
                    cm.getTableName() + "." + cm.getName(),
                    cm.getDataType(),
                    cm.getAlias(),
                    cm.isNullable());

                if (!values.contains(cm2)) {
                    values.add(cm2);
                }
                if (!metaValues.contains(cm)) {
                    metaValues.add(cm);
                }
            }
        }

        // 是否针对重复的value进行排序
        if (sortedDuplicates) {// identity
            if (columns.size() < orderBys.size()) {
                // throw new RuntimeException("should not be here");
                for (IOrderBy ob : orderBys) {
                    if (hashOrderBys.contains(ob)) {
                        continue;
                    } else {
                        ISelectable cm = ob.getColumn();
                        ColumnMeta cm2 = new ColumnMeta(cm.getTableName(),
                            cm.getTableName() + "." + cm.getColumnName(),
                            cm.getDataType(),
                            cm.getAlias(),
                            true);
                        columns.add(cm2);
                        metaColumns.add(cm2);
                    }
                }
            }
            values.add(new ColumnMeta(columns.get(0).getTableName(), identity, DataType.IntegerType, null, true));
        }
    }

    private boolean findOrderByInKey(List<IOrderBy> orderBys, List<ColumnMeta> columns, ColumnMeta cm,
                                     Set<IOrderBy> hashOrderBys) {
        for (IOrderBy ob : orderBys) {
            ISelectable iSelectable = ob.getColumn();
            String orderByTable = iSelectable.getTableName();

            orderByTable = GeneralUtil.getLogicTableName(orderByTable);

            if (cm != null && TStringUtil.equals(GeneralUtil.getLogicTableName(cm.getTableName()), orderByTable)) {
                if (TStringUtil.equals(cm.getName(), iSelectable.getColumnName())) {
                    ColumnMeta cm2 = new ColumnMeta(cm.getTableName(),
                        cm.getTableName() + "." + cm.getName(),
                        cm.getDataType(),
                        cm.getAlias(),
                        cm.isNullable());
                    // 列名与order by Match.放到key里
                    columns.add(cm2);
                    hashOrderBys.add(ob);
                    return true;
                }
            }
        }
        return false;
    }

    public ICursorFactory getCursorFactory() {
        return cursorFactory;
    }

    public void setCursorFactory(ICursorFactory cursorFactory) {
        this.cursorFactory = cursorFactory;

    }

    @Override
    public IRowSet next() throws TddlException {
        initTTSC();
        IRowSet next = parentCursorNext();
        next = wrap(next);
        // System.out.println("TempTableSortCursor: next "+next);
        return next;
    }

    private IRowSet wrap(IRowSet next) {
        if (next != null) {
            next = new RowSetWrapper(returnMeta, next);
        }
        return next;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        initTTSC();
        return parentCursorSkipTo(key);
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        initTTSC();
        return parentCursorSkipTo(key);
    }

    @Override
    public IRowSet current() throws TddlException {
        initTTSC();
        IRowSet current = parentCursorCurrent();
        current = wrap(current);
        return current;
    }

    @Override
    public IRowSet first() throws TddlException {
        initTTSC();
        IRowSet first = parentCursorFirst();
        first = wrap(first);
        return first;
    }

    @Override
    public IRowSet last() throws TddlException {
        initTTSC();
        IRowSet last = parentCursorPrev();
        last = wrap(last);
        return last;
    }

    @Override
    public IRowSet prev() throws TddlException {
        initTTSC();
        IRowSet prev = parentCursorPrev();
        prev = wrap(prev);
        return prev;
    }

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        exs = parentCursorClose(exs);
        if (targetTable != null) try {
            targetTable.close();
        } catch (TddlException e) {
            exs.add(e);
        }
        return exs;
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        sb.append(tabTittle).append("TempTableCursor").append("\n");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);
        if (this.cursor != null) {
            sb.append(this.tempTargetCursor.toStringWithInden(inden + 1));
        }
        return sb.toString();
    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        initTTSC();
        return this.returnMeta.getColumns();
    }
}
