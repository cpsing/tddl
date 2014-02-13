package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.codec.CodecFactory;
import com.taobao.tddl.executor.codec.RecordCodec;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
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
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author mengshi.sunmengshi 2013-12-19 下午12:18:29
 * @since 5.0.0
 */
public class MergeCursor extends SchematicCursor implements IMergeCursor {

    private final Logger                   logger                       = LoggerFactory.getLogger(MergeCursor.class);
    protected List<ISchematicCursor>       cursors;
    protected int                          sizeLimination               = 10000;
    protected final IDataNodeExecutor      currentExecotor;

    protected final ExecutionContext       executionContext;
    protected ValueMappingIRowSetConvertor valueMappingIRowSetConvertor = new ValueMappingIRowSetConvertor();
    protected int                          currentIndex                 = 0;

    public MergeCursor(List<ISchematicCursor> cursors, IDataNodeExecutor currentExecotor,
                       ExecutionContext executionContext){
        super(null, null, null);

        this.currentExecotor = currentExecotor;
        this.executionContext = executionContext;
        this.cursors = cursors;
        List<IOrderBy> orderBys = this.cursors.get(0).getOrderBy();
        setOrderBy(orderBys);
    }

    public MergeCursor(List<ISchematicCursor> cursors, ICursorMeta iCursorMeta, IDataNodeExecutor currentExecotor,
                       ExecutionContext executionContext, List<IOrderBy> orderBys){
        super(null, iCursorMeta, orderBys);
        this.cursors = cursors;

        this.currentExecotor = currentExecotor;
        this.executionContext = executionContext;
        setOrderBy(orderBys);
    }

    @Override
    protected void init() throws TddlException {
        if (this.inited) {
            return;
        }
        super.init();
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

    @Override
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
            // 这里列的别名也丢了吧 似乎解决了
            IQuery iquery = (IQuery) currentExecotor;
            OptimizerContext optimizerContext = OptimizerContext.getContext();
            IBooleanFilter ibf = ASTNodeFactory.getInstance().createBooleanFilter();
            ibf.setOperation(OPERATION.IN);
            ibf.setValues(new ArrayList<Object>());
            String colName = null;
            for (CloneableRecord record : keys) {

                Map<String, Object> recordMap = record.getMap();
                if (recordMap.size() == 1) {
                    // 单字段in
                    Entry<String, Object> entry = recordMap.entrySet().iterator().next();
                    Object comp = entry.getValue();
                    colName = entry.getKey();
                    IColumn col = ASTNodeFactory.getInstance()
                        .createColumn()
                        .setColumnName(colName)
                        .setDataType(record.getType(0));

                    col.setTableName(iquery.getAlias());
                    ibf.setColumn(col);
                    ibf.getValues().add(comp);
                } else {
                    // 多字段in
                    if (ibf.getColumn() == null) {
                        ibf.setColumn(buildRowFunction(recordMap.keySet(), true, record));
                    }

                    ibf.getValues().add(buildRowFunction(recordMap.values(), false, record));

                }
            }

            KVIndexNode query = new KVIndexNode(iquery.getIndexName());
            query.select(iquery.getColumns());
            query.setLimitFrom(iquery.getLimitFrom());
            query.setLimitTo(iquery.getLimitTo());
            query.setOrderBys(iquery.getOrderBys());
            query.setGroupBys(iquery.getGroupBys());
            // query.valueQuery(removeDupFilter(iquery.getValueFilter(), ibf));
            // query.keyQuery(removeDupFilter(iquery.getKeyFilter(), ibf));
            // if (keyFilterOrValueFilter)
            // query.keyQuery(FilterUtils.and(query.getKeyFilter(), ibf));
            // else query.valueQuery(FilterUtils.and(query.getResultFilter(),
            // ibf));

            // 直接构造为where条件，优化器进行重新选择
            IFilter whereFilter = FilterUtils.and(iquery.getKeyFilter(), iquery.getValueFilter());
            query.query(FilterUtils.and(removeDupFilter(whereFilter, ibf), ibf));
            query.alias(iquery.getAlias());
            query.build();
            // IDataNodeExecutor idne = dnc.shard(currentExecotor,
            // Collections.EMPTY_MAP, null);
            IDataNodeExecutor idne = null;
            // 优化做法，将数据分配掉。
            Integer currentThread = currentExecotor.getThread();

            executionContext.getExtraCmds().put("initThread", currentThread);

            // TODO 以后要考虑做cache
            idne = optimizerContext.getOptimizer().optimizeAndAssignment(query,
                executionContext.getParams(),
                executionContext.getExtraCmds());

            ISchematicCursor cursor = null;
            Map<CloneableRecord, DuplicateKVPair> duplicateKeyMap = null;
            try {
                ExecutionContext tempContext = new ExecutionContext();
                tempContext.setCurrentRepository(executionContext.getCurrentRepository());
                tempContext.setExecutorService(executionContext.getExecutorService());
                cursor = ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNode(idne, tempContext);
                // 用于关闭，统一管理
                this.returnColumns = cursor.getReturnColumns();
                List<IColumn> cols = new ArrayList<IColumn>();
                if (ibf.getColumn() instanceof IColumn) {
                    cols.add((IColumn) ibf.getColumn());
                } else {
                    cols.addAll(((IFunction) ibf.getColumn()).getArgs());
                }
                duplicateKeyMap = buildDuplicateKVPairMap(cols, cursor);
            } finally {
                if (cursor != null) {
                    List<TddlException> exs = new ArrayList();
                    exs = cursor.close(exs);
                    if (!exs.isEmpty()) {
                        throw GeneralUtil.mergeException(exs);
                    }
                }
            }
            return duplicateKeyMap;
        }
    }

    private IFunction buildRowFunction(Collection values, boolean isColumn, CloneableRecord record) {
        IFunction func = ASTNodeFactory.getInstance().createFunction();
        func.setFunctionName("ROW");
        StringBuilder columnName = new StringBuilder();
        columnName.append('(').append(StringUtils.join(values, ',')).append(')');
        func.setColumnName(columnName.toString());
        if (isColumn) {
            List<IColumn> columns = new ArrayList<IColumn>(values.size());
            for (Object value : values) {
                IColumn col = ASTNodeFactory.getInstance()
                    .createColumn()
                    .setColumnName((String) value)
                    .setDataType(record.getType((String) value));
                columns.add(col);
            }

            func.setArgs(columns);
        } else {
            func.setArgs(new ArrayList(values));
        }
        return func;
    }

    /**
     * 合并两个条件去除重复的key条件，比如构造了id in (xxx)的请求后，原先条件中有可能也存在id的条件，这时需要替换原先的id条件
     * 
     * @param srcFilter
     * @param mergeFilter
     */
    private IFilter removeDupFilter(IFilter srcFilter, IBooleanFilter inFilter) {
        List<List<IFilter>> filters = FilterUtils.toDNFNodesArray(srcFilter);
        List<List<IFilter>> newFilters = new ArrayList<List<IFilter>>();
        for (List<IFilter> sf : filters) {
            List<IFilter> newSf = new ArrayList<IFilter>();
            for (IFilter f : sf) {
                if (!((IBooleanFilter) f).getColumn().equals(inFilter.getColumn())) {
                    newSf.add(f);
                }
            }

            newFilters.add(newSf);
        }

        return FilterUtils.DNFToOrLogicTree(newFilters);
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
    private Map<CloneableRecord, DuplicateKVPair> buildDuplicateKVPairMap(List<IColumn> cols, ISchematicCursor cursor)
                                                                                                                      throws TddlException {

        Map<CloneableRecord, DuplicateKVPair> duplicateKeyMap = new HashMap<CloneableRecord, DuplicateKVPair>();
        IRowSet kvPair;
        int count = 0;
        List<IColumn> icols = new ArrayList<IColumn>();
        List<ColumnMeta> colMetas = new ArrayList<ColumnMeta>();
        for (IColumn c : cols) {
            ISelectable icol = c.copy();
            icol.setTableName(null);
            colMetas.add(ExecUtils.getColumnMeta(icol));
            icols.add((IColumn) icol);
        }
        RecordCodec codec = CodecFactory.getInstance(CodecFactory.FIXED_LENGTH).getCodec(colMetas);
        while ((kvPair = cursor.next()) != null) {
            kvPair = ExecUtils.fromIRowSetToArrayRowSet(kvPair);
            CloneableRecord key = codec.newEmptyRecord();
            for (IColumn icol : icols) {
                Object v = ExecUtils.getValueByIColumn(kvPair, icol);
                key.put(icol.getColumnName(), v);
            }

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
        if (this.returnColumns != null) {
            return this.returnColumns;
        }

        if (this.cursors != null && !cursors.isEmpty()) {
            this.returnColumns = cursors.get(0).getReturnColumns();
        }

        return this.returnColumns;
    }
}
