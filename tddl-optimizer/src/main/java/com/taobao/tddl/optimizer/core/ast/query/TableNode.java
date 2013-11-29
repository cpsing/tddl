package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.TableNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 查询某个具体的真实表的Node 允许使用这个node，根据查询条件进行树的构建
 * 
 * @author Dreamond
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @since 5.1.0
 */
public class TableNode extends QueryTreeNode {

    private TableNodeBuilder builder;
    private String           tableName;
    private String           actualTableName;              // 比如存在水平分表时，tableName代表逻辑表名,actualTableName代表物理表名
    private IFilter          indexQueryValueFilter = null;
    private TableMeta        tableMeta;
    private IndexMeta        indexUsed             = null; // 当前逻辑表的使用index
    private boolean          fullTableScan         = false; // 是否需要全表扫描

    public TableNode(){
        this(null);
    }

    public TableNode(String tableName){
        this.tableName = tableName;
        builder = new TableNodeBuilder(this);
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);
        this.indexQueryValueFilter = OptimizerUtils.assignment(indexQueryValueFilter, parameterSettings);
    }

    public IQueryTree toDataNodeExecutor() throws QueryException {
        return this.convertToJoinIfNeed().toDataNodeExecutor();
    }

    /**
     * 根据索引信息构建查询树，可能需要进行主键join
     * 
     * <pre>
     * 分支：
     * 1. 没选择索引，直接按照主键进行全表扫描
     * 2. 选择了索引
     *      a. 选择的是主键索引，直接按照主键构造查询
     *      b. 选择的是非主键索引，需要考虑做主键二次join查询
     *          i. 如果索引信息里包含了所有的选择字段，直接基于主键查询返回，一次查询就够了
     *          ii. 包含了非索引中的字段，需要做回表查询. 
     *              先根据索引信息查询到主键，再根据主键查询出所需的其他字段，对应的join条件即为主键字段
     * </pre>
     */
    public QueryTreeNode convertToJoinIfNeed() {
        if (this.getIndexUsed() == null || this.getIndexUsed().isPrimaryKeyIndex()) {
            // 若不包含索引，则扫描主表即可或者使用主键索引
            KVIndexNode keyIndexQuery = new KVIndexNode(this.getTableMeta().getPrimaryIndex().getName());
            // 如果有别名，用别名，否则，用逻辑表名替代索引名
            keyIndexQuery.alias(this.getName());
            keyIndexQuery.setLimitFrom(this.getLimitFrom());
            keyIndexQuery.setLimitTo(this.getLimitTo());
            keyIndexQuery.select(OptimizerUtils.copySelectables(this.getColumnsSelected()));
            keyIndexQuery.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys()));
            keyIndexQuery.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys()));
            keyIndexQuery.having(OptimizerUtils.copyFilter(this.getHavingFilter()));
            keyIndexQuery.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter()));
            keyIndexQuery.keyQuery(OptimizerUtils.copyFilter(this.getKeyFilter()));
            keyIndexQuery.valueQuery(FilterUtils.and(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter()),
                OptimizerUtils.copyFilter(this.getResultFilter())));
            keyIndexQuery.executeOn(this.getDataNode());
            keyIndexQuery.setSubQuery(this.isSubQuery());
            keyIndexQuery.setFullTableScan(this.isFullTableScan());
            keyIndexQuery.build();
            return keyIndexQuery;
        } else { // 非主键索引
            IndexMeta indexUsed = this.getIndexUsed();
            List<ISelectable> indexQuerySelected = new ArrayList<ISelectable>();

            KVIndexNode indexQuery = new KVIndexNode(this.getIndexUsed().getName());
            indexQuery.alias(indexUsed.getNameWithOutDot());
            indexQuery.keyQuery(OptimizerUtils.copyFilter(this.getKeyFilter()));
            indexQuery.valueQuery(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter()));
            // 索引是否都包含在查询字段中
            boolean isIndexCover = true;
            List<ISelectable> allColumnsRefered = this.getColumnsRefered();
            for (ISelectable selected : allColumnsRefered) {
                if (selected instanceof IFunction) {
                    continue;
                }

                ColumnMeta cm = indexUsed.getColumnMeta(selected.getColumnName());
                if (cm == null) {
                    isIndexCover = false;
                } else {
                    indexQuerySelected.add(ASTNodeFactory.getInstance()
                        .createColumn()
                        .setColumnName(selected.getColumnName()));
                }
            }
            indexQuery.select(indexQuerySelected);
            // 索引覆盖的情况下，只需要返回索引查询
            if (isIndexCover) {
                indexQuery.select(OptimizerUtils.copySelectables(this.getColumnsSelected()));
                indexQuery.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys()));
                indexQuery.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys()));
                indexQuery.setLimitFrom(this.getLimitFrom());
                indexQuery.setLimitTo(this.getLimitTo());
                indexQuery.executeOn(this.getDataNode());
                indexQuery.alias(this.getName());
                indexQuery.setSubQuery(this.isSubQuery());
                indexQuery.having(OptimizerUtils.copyFilter(this.getHavingFilter()));
                indexQuery.valueQuery(FilterUtils.and(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter()),
                    OptimizerUtils.copyFilter(this.getResultFilter())));
                indexQuery.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter()));
                indexQuery.build();
                return indexQuery;
            } else {
                // 不是索引覆盖的情况下，需要回表，就是索引查询和主键查询
                IndexMeta pk = this.getTableMeta().getPrimaryIndex();
                // 由于按照主键join，主键也是被引用的列
                for (ColumnMeta keyColumn : pk.getKeyColumns()) {
                    boolean has = false;
                    for (ISelectable s : allColumnsRefered) {
                        if (keyColumn.getName().equals(s.getColumnName())) {
                            has = true;
                            break;
                        }
                    }

                    if (!has) {// 不存在索引字段
                        allColumnsRefered.add(ASTNodeFactory.getInstance()
                            .createColumn()
                            .setColumnName(keyColumn.getName()));
                        indexQuery.addColumnsSelected(ASTNodeFactory.getInstance()
                            .createColumn()
                            .setColumnName(keyColumn.getName()));
                    }
                }

                List<ISelectable> keyQuerySelected = new ArrayList<ISelectable>();
                KVIndexNode keyQuery = new KVIndexNode(pk.getName());
                keyQuery.alias(this.getName());
                for (ISelectable selected : allColumnsRefered) {
                    // 函数应该回表的时候做
                    if (selected instanceof IFunction) {
                        continue;
                    }

                    keyQuerySelected.add(ASTNodeFactory.getInstance()
                        .createColumn()
                        .setColumnName(selected.getColumnName()));
                }
                keyQuery.select(keyQuerySelected);
                // mengshi 如果valueFilter中有index中的列，实际应该在indexQuery中做
                keyQuery.valueQuery(OptimizerUtils.copyFilter(this.getResultFilter()));

                JoinNode join = indexQuery.join(keyQuery);
                // 按照PK进行join
                for (ColumnMeta keyColumn : pk.getKeyColumns()) {
                    IBooleanFilter eq = ASTNodeFactory.getInstance().createBooleanFilter();
                    eq.setOperation(OPERATION.EQ);
                    eq.setColumn(ASTNodeFactory.getInstance()
                        .createColumn()
                        .setColumnName(keyColumn.getName())
                        .setTableName(indexUsed.getName()));
                    eq.setValue(ASTNodeFactory.getInstance()
                        .createColumn()
                        .setColumnName(keyColumn.getName())
                        .setTableName(pk.getName()));
                    join.addJoinFilter(eq);
                }

                List<ISelectable> columns = new ArrayList<ISelectable>();
                if (this.getAlias() == null) {
                    columns = OptimizerUtils.copySelectables(this.getColumnsSelected());
                } else {
                    for (ISelectable s : this.getColumnsSelected()) {
                        ISelectable a = s.copy().setTableName(this.getAlias());
                        if (a instanceof IFunction && this.getAlias() != null) {
                            this.findColumnInFunctionArgAndSetTableNameAsAlias((IFunction) a.copy());
                        }

                        columns.add(a);
                    }
                }

                join.select(columns);
                List<IOrderBy> orderBys = new ArrayList<IOrderBy>(this.getOrderBys().size());
                for (IOrderBy o : this.getOrderBys()) {
                    IOrderBy newO = o.deepCopy();
                    if (o.getColumn().getAlias() != null) {
                        newO.getColumn().setColumnName(o.getColumn().getAlias());
                    }
                    if (this.getAlias() != null) {
                        newO.getColumn().setTableName(this.getAlias());
                    }
                    orderBys.add(newO);
                }

                join.setOrderBys(orderBys);
                List<IOrderBy> groupBys = new ArrayList<IOrderBy>(this.getGroupBys().size());
                for (IOrderBy group : this.getGroupBys()) {
                    IOrderBy newG = group.copy();
                    if (group.getAlias() != null) {
                        newG.setColumnName(group.getAlias());
                    }

                    if (this.getAlias() != null) {
                        newG.setTableName(this.getAlias());
                    }
                    groupBys.add(newG);
                }

                join.setUsedForIndexJoinPK(true);
                join.setGroupBys(groupBys);
                join.setLimitFrom(this.getLimitFrom());
                join.setLimitTo(this.getLimitTo());
                join.executeOn(this.getDataNode());
                join.setSubQuery(this.isSubQuery());
                // 回表是IndexNestedLoop
                join.setJoinStrategy(new IndexNestedLoopJoin());
                join.setAlias(this.getAlias());
                join.having(OptimizerUtils.copyFilter(this.getHavingFilter()));
                join.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter()));
                join.build();
                return join;
            }
        }
    }

    // select count(id) from table1 as t1
    // ->select count(t1.id)
    private void findColumnInFunctionArgAndSetTableNameAsAlias(IFunction f) {
        for (Object arg : f.getArgs()) {
            if (arg instanceof ISelectable) {
                ((ISelectable) arg).setTableName(this.getAlias());
                if (arg instanceof IFunction) {
                    this.findColumnInFunctionArgAndSetTableNameAsAlias((IFunction) arg);
                }
            }

        }
    }

    public List getImplicitOrderBys() {
        // 如果有显示group by，直接使用group by
        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        } else {
            // 默认使用主键的索引信息进行order by
            List<IOrderBy> implicitOrdersCandidate = OptimizerUtils.getOrderBy(this.tableMeta.getPrimaryIndex());
            List<IOrderBy> implicitOrders = new ArrayList();
            for (int i = 0; i < implicitOrdersCandidate.size(); i++) {
                if (this.getColumnsSelected().contains(implicitOrdersCandidate.get(i).getColumn())) {
                    implicitOrders.add(implicitOrdersCandidate.get(i));
                } else {
                    break;
                }
            }
            return implicitOrders;
        }
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public String getName() {
        if (this.getAlias() != null) {
            return this.getAlias();
        }
        return this.getTableName();
    }

    protected ISelectable getColumn(String name) {
        if (this.getTableMeta() == null) {
            this.build();
        }

        return this.getBuilder()
            .getSelectableFromChild(ASTNodeFactory.getInstance().createColumn().setColumnName(name));
    }

    // ============= insert/update/delete/put==================

    public InsertNode insert(List<ISelectable> columns, List<Comparable> values) {
        InsertNode insert = new InsertNode(this);
        insert.setColumns(columns);
        insert.setValues(values);

        return insert;
    }

    public InsertNode insert(String columns, Comparable values[]) {
        if (columns == null || columns.isEmpty()) {
            return this.insert(new String[] {}, values);
        }
        return this.insert(columns.split(" "), values);
    }

    public InsertNode insert(String columns[], Comparable values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Comparable> valueList = new ArrayList<Comparable>(Arrays.asList(values));
        return this.insert(cs, valueList);
    }

    public PutNode put(List<ISelectable> columns, List<Comparable> values) {
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("The size of the columns and values is not matched."
                                               + " columns' size is " + columns.size() + ". values' size is "
                                               + values.size());
        }

        PutNode put = new PutNode(this);
        put.setColumns(columns);
        put.setValues(values);
        return put;
    }

    public PutNode put(String columns, Comparable values[]) {
        return put(columns.split(" "), values);
    }

    public PutNode put(String columns[], Comparable values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Comparable> valueList = new ArrayList<Comparable>(Arrays.asList(values));
        return put(cs, valueList);
    }

    public UpdateNode update(List<ISelectable> columns, List<Comparable> values) {

        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("The size of the columns and values is not matched."
                                               + " columns' size is " + columns.size() + ". values' size is "
                                               + values.size());
        }

        UpdateNode update = new UpdateNode(this);
        update.setUpdateValues(values);
        update.setUpdateColumns(columns);
        return update;
    }

    public UpdateNode update(String columns, Comparable values[]) {
        return update(columns.split(" "), values);
    }

    public UpdateNode update(String columns[], Comparable values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Comparable> valueList = new ArrayList<Comparable>(Arrays.asList(values));
        return update(cs, valueList);
    }

    public DeleteNode delete() {
        DeleteNode delete = new DeleteNode(this);
        return delete;
    }

    // =============== copy =============
    public TableNode copy() {
        TableNode newTableNode = new TableNode(this.getTableName());
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    protected void copySelfTo(QueryTreeNode to) {
        super.copySelfTo(to);
        TableNode toTable = (TableNode) to;
        toTable.setFullTableScan(this.isFullTableScan());
        toTable.setIndexQueryValueFilter((IFilter) (indexQueryValueFilter == null ? null : indexQueryValueFilter.copy()));
        toTable.setTableMeta(this.getTableMeta());
        toTable.useIndex(this.getIndexUsed());
    }

    public TableNode deepCopy() {
        TableNode newTableNode = new TableNode(this.getTableName());
        this.deepCopySelfTo(newTableNode);
        return newTableNode;
    }

    protected void deepCopySelfTo(QueryTreeNode to) {
        super.deepCopySelfTo(to);
        TableNode toTable = (TableNode) to;
        toTable.setFullTableScan(this.isFullTableScan());
        toTable.setIndexQueryValueFilter((IFilter) (indexQueryValueFilter == null ? null : indexQueryValueFilter.copy()));
        toTable.tableName = this.tableName;
        toTable.setTableMeta(this.getTableMeta());
        toTable.useIndex(null);
    }

    // ============== setter / getter==================

    public boolean isFullTableScan() {
        return this.fullTableScan;
    }

    public void setFullTableScan(boolean fullTableScan) {
        this.fullTableScan = fullTableScan;
    }

    public IndexMeta getIndexUsed() {
        return indexUsed;
    }

    public TableNode useIndex(IndexMeta index) {
        this.indexUsed = index;
        return this;
    }

    public List<IndexMeta> getIndexs() {
        return this.getTableMeta().getIndexs();
    }

    public String getTableName() {
        return this.tableName;
    }

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public IFilter getIndexQueryValueFilter() {
        return indexQueryValueFilter;
    }

    public void setIndexQueryValueFilter(IFilter indexValueFilter) {
        this.indexQueryValueFilter = indexValueFilter;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getActualTableName() {
        return actualTableName;
    }

    public void setActualTableName(String actualTableName) {
        this.actualTableName = actualTableName;
    }

    public String toString(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "Query from " + this.getTableName() + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "Query from " + this.getTableName());
        }

        appendField(sb, "actualTableName", this.getActualTableName(), tabContent);
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter()), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getResultFilter()), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter()), tabContent);
        appendField(sb, "indexQueryValueFilter", printFilterString(this.getIndexQueryValueFilter()), tabContent);
        appendField(sb, "otherJoinOnFilter", printFilterString(this.getOtherJoinOnFilter()), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter()), tabContent);
        appendField(sb, "indexUsed", this.getIndexUsed(), tabContent);
        if (!(this.getLimitFrom() != null && this.getLimitFrom().equals(0L) && this.getLimitTo() != null && this.getLimitTo()
            .equals(0L))) {
            appendField(sb, "limitFrom", this.getLimitFrom(), tabContent);
            appendField(sb, "limitTo", this.getLimitTo(), tabContent);
        }

        if (this.isSubQuery()) {
            appendField(sb, "isSubQuery", this.isSubQuery(), tabContent);
        }

        appendField(sb, "orderBy", this.getOrderBys(), tabContent);
        appendField(sb, "queryConcurrency", this.getQueryConcurrency(), tabContent);
        appendField(sb, "lockModel", this.getLockModel(), tabContent);
        appendField(sb, "columns", this.getColumnsSelected(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "sql", this.getSql(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);
        return sb.toString();
    }
}
