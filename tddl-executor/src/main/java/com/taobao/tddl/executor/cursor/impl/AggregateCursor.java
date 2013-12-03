package com.taobao.tddl.executor.cursor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.IAggregateCursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.IRowSet;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * 用来计算聚合函数，group by
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:53:31
 * @since 5.1.0
 */
public class AggregateCursor extends SchematicCursor implements IAggregateCursor {

    private final static Log  logger                    = LogFactory.getLog(AggregateCursor.class);
    /**
     * 查询中涉及的所有聚合函数
     */
    protected List<IFunction> aggregates                = new LinkedList<IFunction>();
    /**
     * 查询中设计的所有scalar函数
     */
    List<IFunction>           scalars                   = new LinkedList<IFunction>();
    /**
     * 当前节点是不是归并节点
     */
    boolean                   isMerge                   = false;
    List<ColumnMeta>          groupBys                  = new ArrayList<ColumnMeta>();
    Map<ColumnMeta, Object>   currentGroupByValue       = null;
    private boolean           schemaInited              = false;
    private ICursorMeta       cursorMeta                = null;

    boolean                   end                       = false;
    IRowSet                   firstRowSetInCurrentGroup = null;
    boolean                   isFirstTime               = true;

    public AggregateCursor(ISchematicCursor cursor, List<IFunction> functions, List<IOrderBy> groupBycols,
                           List<ISelectable> retColumns, boolean isMerge){
        super(cursor, null, cursor.getOrderBy());

        this.groupBys.addAll(ExecUtil.getColumnMetaWithLogicTablesFromOrderBys(groupBycols));
        for (IFunction f : functions) {
            if (f.getFunctionType().equals(FunctionType.Scalar)) this.scalars.add(f);
        }
        this.aggregates.addAll(this.getAllAggregates(functions));
        this.isMerge = isMerge;

    }

    @Override
    public IRowSet next() throws Exception {
        initSchema();
        if (end) return null;

        if (isFirstTime) {

            if (firstRowSetInCurrentGroup == null) {
                end = true;
                return null;
            }
            isFirstTime = false;

            for (IFunction aggregate : aggregates) {
                aggregate.clear();
            }
        }

        // 初始化currentGroupByValue，并把当前第一条记录中的值放进去
        if (this.groupBys != null && !this.groupBys.isEmpty()) {
            if (this.currentGroupByValue == null) {
                this.currentGroupByValue = new HashMap();
            }

            for (ColumnMeta cm : groupBys) {
                if (firstRowSetInCurrentGroup == null) currentGroupByValue = null;
                else {
                    Object value = ExecUtil.getObject(firstRowSetInCurrentGroup.getParentCursorMeta(),
                        firstRowSetInCurrentGroup,
                        cm.getTableName(),
                        cm.getName());
                    currentGroupByValue.put(cm, value);
                }
            }
        }

        IRowSet record = new ArrayRowSet(cursorMeta.getColumns().size(), cursorMeta);
        IRowSet kv = firstRowSetInCurrentGroup;

        // 这里无论KV是否是null，第一次都应该让函数来处理
        if (kv == null && aggregates.isEmpty()) {
            end = true;
            return null;
        }

        if (kv != null) {
            // cursorMeta后面有函数，所以以kv的meta为准
            for (int i = 0; i < kv.getParentCursorMeta().getColumns().size(); i++) {
                ColumnMeta cm = cursorMeta.getColumns().get(i);
                Integer index = kv.getParentCursorMeta().getIndex(cm.getTableName(), cm.getName());
                if (index == null) index = kv.getParentCursorMeta().getIndex(cm.getTableName(), cm.getAlias());
                record.setObject(i, kv.getObject(index));
            }
        }

        if (!aggregates.isEmpty() || (this.groupBys != null && !this.groupBys.isEmpty())) {
            do {

                // 如果组的值发生了变化，则返回一条记录
                if (isCurrentGroupByChanged(kv)) {
                    this.firstRowSetInCurrentGroup = kv;
                    break;
                }
                for (IFunction aggregate : aggregates) {
                    if (this.isMerge() && !aggregate.isNeedDistinctArg()) {
                        aggregate.serverReduce(kv);
                    } else aggregate.serverMap(kv);
                }
            } while ((kv = super.next()) != null);
        } else {
            kv = super.next();
            firstRowSetInCurrentGroup = kv;
        }

        // 将函数的结果放到结果集中
        this.putFunctionsResultInRecord(aggregates, record);

        // 对于aggregate函数，需要遍历所有结果集
        // 当两者同时存在时，scalar函数只处理第一条结果
        // mysql是这样做的
        // 对于scalar函数，只需要取一条结果
        for (IFunction scalar : this.scalars) {
            if (this.isMerge()) {
                scalar.serverReduce(record);
            } else scalar.serverMap(record);
        }
        this.putFunctionsResultInRecord(scalars, record);

        for (IFunction aggregate : aggregates) {
            aggregate.clear();
        }

        end = (kv == null);
        return record;
    }

    // 递归遍历所有给定的函数，得到其中的聚合函数
    // 包括参数中的函数
    public List<IFunction> getAllAggregates(List<IFunction> functions) {
        List<IFunction> aggregates = new LinkedList<IFunction>();

        for (IFunction f : functions) {
            List<IFunction> functionsInArgs = new ArrayList<IFunction>(f.getArgs().size());

            for (Object arg : f.getArgs()) {
                if (arg instanceof IFunction) {
                    functionsInArgs.add((IFunction) arg);
                }
            }
            List<IFunction> aggregatesInArgs = this.getAllAggregates(functionsInArgs);
            aggregates.addAll(aggregatesInArgs);

            if (f.getFunctionType().equals(FunctionType.Aggregate)) {
                aggregates.add(f);

                // 聚合函数不能使用聚合函数作为参数
                // 如 max(count(id))是错误的
                if (!aggregatesInArgs.isEmpty()) {
                    throw new RuntimeException("Invalid use of group function");
                }
            }
        }

        return aggregates;
    }

    private void initSchema() throws Exception {
        if (schemaInited) {
            return;
        }
        schemaInited = true;
        firstRowSetInCurrentGroup = super.next();

        if (firstRowSetInCurrentGroup == null) return;
        // 把聚合的结果放在最后
        ICursorMeta meta = firstRowSetInCurrentGroup.getParentCursorMeta();
        List<ColumnMeta> retColumns = new ArrayList<ColumnMeta>(meta.getColumns().size() + this.aggregates.size());

        retColumns.addAll(meta.getColumns());

        for (IFunction c : this.aggregates) {
            Integer index = meta.getIndex(c.getTableName(), c.getColumnName());

            if (index == null) index = meta.getIndex(c.getTableName(), c.getAlias());
            if (index == null) putRetColumnInMeta(c, retColumns);
        }

        for (IFunction c : this.scalars) {
            Integer index = meta.getIndex(c.getTableName(), c.getColumnName());
            if (index == null) index = meta.getIndex(c.getTableName(), c.getAlias());
            if (index == null) putRetColumnInMeta(c, retColumns);
        }
        cursorMeta = CursorMetaImp.buildNew(retColumns, retColumns.size());

        if (AndorLogManager.isDebugMode()) {
            logger.warn("firstRowSetInCurrentGroup:\n" + firstRowSetInCurrentGroup);
            logger.warn("cursorMeta:\n" + cursorMeta);
        }
    }

    private boolean isCurrentGroupByChanged(IRowSet kv) {

        if (this.groupBys != null && !this.groupBys.isEmpty()) {
            if (this.currentGroupByValue == null) {
                return false;
            }
            if (kv == null) return true;

            for (ColumnMeta cm : this.currentGroupByValue.keySet()) {
                Object valueFromKv = ExecUtil.getObject(kv.getParentCursorMeta(), kv, cm.getTableName(), cm.getName());
                Object valueCurrent = this.currentGroupByValue.get(cm);
                if (valueFromKv == null) {
                    if (valueCurrent != null) return true;
                } else {
                    if (valueCurrent == null) return true;
                    if (!valueFromKv.equals(valueCurrent)) return true;
                }
            }
        }

        return false;
    }

    public IRowSet first() throws Exception {
        this.end = false;
        this.isFirstTime = true;
        super.beforeFirst();

        return this.next();

    }

    public void beforeFirst() throws Exception {
        schemaInited = false;
        this.end = false;
        this.isFirstTime = true;
        super.beforeFirst();
    }

    public boolean isMerge() {
        return this.isMerge;
    }

    public void setMerge(boolean isMerge) {
        this.isMerge = isMerge;
    }

    void putFunctionsResultInRecord(List<IFunction> functions, IRowSet record) {

        for (IFunction f : functions) {

            Integer index = this.cursorMeta.getIndex(f.getTableName(), f.getColumnName());
            if (index == null) index = this.cursorMeta.getIndex(f.getTableName(), f.getAlias());
            Object res = f.getResult();
            if (res instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) f.getResult();
                for (Entry<String, Object> en : map.entrySet()) {
                    record.setObject(index, en.getValue());
                }
            } else {
                record.setObject(index, res);
            }
        }
    }

    void putRetColumnInMeta(ISelectable column, List<ColumnMeta> metaColumns) {

        String columnName;

        columnName = column.getColumnName();
        DATA_TYPE type = null;

        // 函数在Map和Reduce过程中的返回类型可以不同
        // 如Avg，map过程返回String
        // reduce过程中返回数字类型
        if (this.isMerge()) type = column.getDataType();
        else {
            if (column instanceof IFunction) {
                type = ((IFunction) column).getMapReturnType();
            } else {
                type = column.getDataType();
            }
        }

        ColumnMeta cm = new ColumnMeta(GeneralUtil.getLogicTableName(column.getTableName()),
            columnName,
            type,
            column.getAlias());
        metaColumns.add(cm);

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String toStringWithInden(int inden) {
        try {
            initSchema();
        } catch (Exception e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【Aggregate cursor . agg funcs").append(aggregates).append("\n");
        GeneralUtil.printMeta(cursorMeta, inden, sb);
        GeneralUtil.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }
}
