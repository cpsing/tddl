package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.IMergeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class MergeCursor extends SchematicCursor implements IMergeCursor {

    private Log                            logger                       = LogFactory.getLog(MergeCursor.class);
    protected List<ISchematicCursor>       cursors;
    int                                    sizeLimination               = 10000;
    protected final IDataNodeExecutor      currentExecotor;

    protected final ExecutionContext       executionContext;
    protected ValueMappingIRowSetConvertor valueMappingIRowSetConvertor = new ValueMappingIRowSetConvertor();
    protected int                          currentIndex                 = 0;

    // /**
    // * mget中用来做查找条件的codec 技术债
    // */
    // protected RecordCodec finderCodec = null;

    public MergeCursor(List<ISchematicCursor> cursors, IDataNodeExecutor currentExecotor,
                       ExecutionContext executionContext){
        super(null, null, null);

        this.currentExecotor = currentExecotor;
        this.executionContext = executionContext;
        this.cursors = cursors;
        List<IOrderBy> orderBys = this.cursors.get(0).getOrderBy();
        setOrderBy(orderBys);
        // buildFinderCodec(orderBys);

    }

    public MergeCursor(List<ISchematicCursor> cursors, ICursorMeta iCursorMeta, IDataNodeExecutor currentExecotor,
                       ExecutionContext executionContext, List<IOrderBy> orderBys){
        super(null, iCursorMeta, orderBys);
        this.cursors = cursors;

        this.currentExecotor = currentExecotor;
        this.executionContext = executionContext;
        setOrderBy(orderBys);
        // buildFinderCodec(orderBys);
    }

    // private void buildFinderCodec(List<IOrderBy> orderBys) {
    // List<IOrderBy> ordersForMeta = new ArrayList<IOrderBy>();
    // if (orderBys != null && !orderBys.isEmpty())
    // ordersForMeta.add(orderBys.get(0));
    // List<ColumnMeta> colMetas =
    // generateMergeOrderByAndReturnColumnList(ordersForMeta);
    // finderCodec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH)
    // .getCodec(colMetas);
    // }

    // private List<ColumnMeta> generateMergeOrderByAndReturnColumnList(
    // List<IOrderBy> orderBys) {
    // List<IOrderBy> newOrderBys = new ArrayList<IOrderBy>();
    // List<ColumnMeta> colMetas = new ArrayList<ColumnMeta>();
    //
    // for (IOrderBy orderBy : orderBys) {
    // OrderBy ob = new OrderBy();
    // ob.setColumn(orderBy.getColumn());
    // // 排序不固定，所以desc asc 判断为null
    // newOrderBys.add(ob);
    // colMetas.add(GeneralUtil.getColumnMeta(ob.getColumn()));
    // }
    // if (!newOrderBys.isEmpty()) {
    // setOrderBy(newOrderBys);
    // }
    // return colMetas;
    // }

    @Override
    protected void init() throws TddlException {
        if (this.inited) return;

        super.init();

        // getReturnColumns();
    }

    @Override
    public IRowSet next() throws TddlException {
        init();
        /*
         * 因为subCursor和first Cursor的meta数据可能排列的顺序不一样。 比如，cursor1 ,顺序可能是pk ,
         * Name. 而cursor 2 ,顺序却是反过来的 ， Name , pk 这时候在这里需要统一Cursor内的meta信息才可以。
         */

        IRowSet iRowSet = innerNext();

        return iRowSet;
    }

    private IRowSet innerNext() throws TddlException {
        init();
        IRowSet ret;
        while (true) {
            if (currentIndex >= cursors.size()) {// 取尽所有cursor.
                return null;
            }
            ISchematicCursor isc = cursors.get(currentIndex);
            ret = isc.next();
            if (ret != null) {
                ret = valueMappingIRowSetConvertor.wrapValueMappingIRowSetIfNeed(ret);
                return ret;
            }

            switchCursor();
        }
    }

    private void switchCursor() {
        cursors.get(currentIndex).close(exceptionsWhenCloseSubCursor);
        currentIndex++;
        // 因为每一个cursor的valueMappingMap都不一样，所以这里把这个数据弄成空的
        valueMappingIRowSetConvertor.reset();
    }

    List<TddlException>      exceptionsWhenCloseSubCursor = new ArrayList();
    private List<ColumnMeta> returnColumns                = null;

    @Override
    public List<TddlException> close(List<TddlException> exs) {
        exs.addAll(exceptionsWhenCloseSubCursor);
        TddlException e = null;

        for (ISchematicCursor _cursor : cursors) {
            exs = _cursor.close(exs);
        }
        return exs;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        init();
        return super.skipTo(key);
    }

    public List<ISchematicCursor> getISchematicCursors() {
        return cursors;
    }

    @Override
    public List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                       boolean keyFilterOrValueFilter) throws TddlException {
        logger.error("do mgetWith Duplicatelist in mergeCursor. should not be here");
        init();
        Map<CloneableRecord, DuplicateKVPair> map = parentCursorMgetWithDuplicate(keys,
            prefixMatch,
            keyFilterOrValueFilter);
        return new ArrayList<DuplicateKVPair>(map.values());
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        init();
        if (prefixMatch) {
            throw new UnsupportedOperationException("not supported yet");
        } else {
            OptimizerContext optimizerContext = OptimizerContext.getContext();

            IBooleanFilter ibf = ASTNodeFactory.getInstance().createBooleanFilter();
            List<Comparable> values = new ArrayList<Comparable>();
            String colName = null;
            for (CloneableRecord record : keys) {
                Map<String, Object> recordMap = record.getMap();
                if (recordMap.size() != 1) {
                    throw new IllegalArgumentException("目前只支持单值查询吧。。简化一点");
                }
                Map<String, Object> map = record.getMap();
                Entry<String, Object> entry = map.entrySet().iterator().next();
                Comparable comp = (Comparable) entry.getValue();
                colName = entry.getKey();

                values.add(comp);
            }

            // 这里列的别名也丢了吧 似乎解决了

            IQuery iquery = (IQuery) currentExecotor;

            KVIndexNode query = new KVIndexNode(iquery.getIndexName());
            query.select(iquery.getColumns());
            query.setLimitFrom(iquery.getLimitFrom());
            query.setLimitTo(iquery.getLimitTo());
            query.setOrderBys(iquery.getOrderBys());
            query.setGroupBys(iquery.getGroupBys());
            query.valueQuery(iquery.getResultSetFilter());
            query.setKeyFilter(iquery.getKeyFilter());
            // ICursorMeta indexMeta = super.cursormeta;
            IColumn col = ASTNodeFactory.getInstance()
                .createColumn()
                .setColumnName(colName)
                .setDataType(DATA_TYPE.LONG_VAL);

            col.setTableName(iquery.getAlias());
            ibf.setColumn(col);
            ibf.setValues(values);
            ibf.setOperation(OPERATION.IN);

            if (keyFilterOrValueFilter) query.keyQuery(FilterUtils.and(query.getKeyFilter(), ibf));
            else query.valueQuery(FilterUtils.and(query.getResultFilter(), ibf));

            query.alias(iquery.getAlias());
            query.build();
            // IDataNodeExecutor idne = dnc.shard(currentExecotor,
            // Collections.EMPTY_MAP, null);
            IDataNodeExecutor idne = null;
            // 优化做法，将数据分配掉。
            Integer currentThread = currentExecotor.getThread();

            executionContext.getExtraCmds().put("initThread", currentThread);

            idne = optimizerContext.getOptimizer().optimizeAndAssignment(query,
                executionContext.getParams(),
                executionContext.getExtraCmds());

            ISchematicCursor cursor = null;
            List<KVPair> returnList;
            Map<CloneableRecord, DuplicateKVPair> duplicateKeyMap = null;
            try {

                ExecutionContext tempContext = new ExecutionContext();
                tempContext.setCurrentRepository(executionContext.getCurrentRepository());
                cursor = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(idne, tempContext);
                // 用于关闭，统一管理
                this.returnColumns = cursor.getReturnColumns();
                duplicateKeyMap = buildDuplicateKVPairMap(col, cursor);

            } finally {
                if (cursor != null) {
                    List<TddlException> exs = new ArrayList();
                    exs = cursor.close(exs);
                    if (!exs.isEmpty()) throw GeneralUtil.mergeException(exs);
                }
            }
            return duplicateKeyMap;
        }
    }

    /**
     * 根据返回结果，创建重复值的kvpairMap
     * 
     * @param keys
     * @param cursor
     * @param duplicateKeyMap
     * @return
     * @throws TddlException
     */
    private Map<CloneableRecord, DuplicateKVPair> buildDuplicateKVPairMap(IColumn c, ISchematicCursor cursor)
                                                                                                             throws TddlException {

        Map<CloneableRecord, DuplicateKVPair> duplicateKeyMap = new HashMap<CloneableRecord, DuplicateKVPair>();
        IRowSet kvPair;
        int count = 0;
        // if (orderBys.size() != 1) {
        // throw new IllegalArgumentException("目前只支持单值查询吧。。简化一点");
        // }
        ISelectable icol = c.copy();
        // for (IOrderBy obys : orderBys) {
        // icol = obys.getColumn().copy();
        icol.setTableName(null);
        // \\ }

        List<ColumnMeta> colMetas = new ArrayList();
        colMetas.add(ExecUtils.getColumnMeta(icol));
        RecordCodec codec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(colMetas);

        IColumn col = ExecUtils.getIColumn(icol);
        while ((kvPair = cursor.next()) != null) {
            kvPair = ExecUtils.fromIRowSetToArrayRowSet(kvPair);
            Object v = ExecUtils.getValueByIColumn(kvPair, (IColumn) icol);

            CloneableRecord key = codec.newEmptyRecord();
            key.put(col.getColumnName(), v);
            DuplicateKVPair tempKVPair = duplicateKeyMap.get(key);
            if (tempKVPair == null) {// 加新列
                tempKVPair = new DuplicateKVPair(kvPair);
                duplicateKeyMap.put(key, tempKVPair);
            } else {// 加重复列
                while (tempKVPair.next != null) {
                    tempKVPair = tempKVPair.next;
                }
                tempKVPair.next = new DuplicateKVPair(kvPair);
            }
            count++;
            if (count >= sizeLimination) {// 保护。。。别太多了
                throw new IllegalArgumentException("size is more than limination " + sizeLimination);
            }
            // returnList.add(kvPair);
        }

        return duplicateKeyMap;
    }

    @Override
    public void beforeFirst() throws TddlException {
        init();
        for (int i = 0; i < cursors.size(); i++) {
            cursors.get(i).beforeFirst();
        }
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

        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "MergeCursor ");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);

        for (ISchematicCursor cursor : cursors) {
            sb.append(cursor.toStringWithInden(inden + 1));
        }
        return sb.toString();

    }

    @Override
    public List<ColumnMeta> getReturnColumns() throws TddlException {
        if (this.returnColumns != null) return this.returnColumns;
        if (this.cursors != null && !cursors.isEmpty()) {
            this.returnColumns = cursors.get(0).getReturnColumns();
        }

        return this.returnColumns;
    }
}
