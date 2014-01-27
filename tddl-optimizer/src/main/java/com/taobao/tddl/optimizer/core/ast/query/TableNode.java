package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

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
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 查询某个具体的真实表的Node 允许使用这个node，根据查询条件进行树的构建
 * 
 * @author Dreamond
 * @author whisper
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 * @since 5.0.0
 */
public class TableNode extends QueryTreeNode {

    private final TableNodeBuilder builder;
    private String                 tableName;
    private String                 actualTableName;              // 比如存在水平分表时，tableName代表逻辑表名,actualTableName代表物理表名
    private IFilter                indexQueryValueFilter = null;
    private TableMeta              tableMeta;
    private IndexMeta              indexUsed             = null; // 当前逻辑表的使用index
    private boolean                fullTableScan         = false; // 是否需要全表扫描

    public TableNode(){
        this(null);
    }

    public TableNode(String tableName){
        this.tableName = tableName;
        builder = new TableNodeBuilder(this);
    }

    @Override
    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    @Override
    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);
        this.indexQueryValueFilter = OptimizerUtils.assignment(indexQueryValueFilter, parameterSettings);
    }

    @Override
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
    @Override
    public QueryTreeNode convertToJoinIfNeed() {
        if (this.getIndexUsed() == null || this.getIndexUsed().isPrimaryKeyIndex()) {
            // 若不包含索引，则扫描主表即可或者使用主键索引
            KVIndexNode keyIndexQuery = new KVIndexNode(this.getTableMeta().getPrimaryIndex().getName());
            // 如果有别名，用别名，否则，用逻辑表名替代索引名
            keyIndexQuery.alias(this.getName());
            keyIndexQuery.setLimitFrom(this.getLimitFrom());
            keyIndexQuery.setLimitTo(this.getLimitTo());
            keyIndexQuery.select(OptimizerUtils.copySelectables(this.getColumnsSelected(), keyIndexQuery.getAlias()));
            keyIndexQuery.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys(), keyIndexQuery.getAlias()));
            keyIndexQuery.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys(), keyIndexQuery.getAlias()));
            keyIndexQuery.having(OptimizerUtils.copyFilter(this.getHavingFilter(), keyIndexQuery.getAlias()));
            keyIndexQuery.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter(),
                keyIndexQuery.getAlias()));
            keyIndexQuery.keyQuery(OptimizerUtils.copyFilter(this.getKeyFilter(), keyIndexQuery.getAlias()));
            keyIndexQuery.valueQuery(FilterUtils.and(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter(),
                keyIndexQuery.getAlias()), OptimizerUtils.copyFilter(this.getResultFilter(), keyIndexQuery.getAlias())));
            keyIndexQuery.executeOn(this.getDataNode());
            keyIndexQuery.setSubQuery(this.isSubQuery());
            keyIndexQuery.setFullTableScan(this.isFullTableScan());
            keyIndexQuery.build();
            return keyIndexQuery;
        } else { // 非主键索引
            IndexMeta indexUsed = this.getIndexUsed();
            List<ISelectable> indexQuerySelected = new ArrayList<ISelectable>();

            KVIndexNode indexQuery = new KVIndexNode(this.getIndexUsed().getName());
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
                indexQuery.alias(this.getName());
                indexQuery.keyQuery(OptimizerUtils.copyFilter(this.getKeyFilter(), indexQuery.getAlias()));
                indexQuery.valueQuery(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter(), indexQuery.getAlias()));
                indexQuery.select(OptimizerUtils.copySelectables(this.getColumnsSelected(), indexQuery.getAlias()));
                indexQuery.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys(), indexQuery.getAlias()));
                indexQuery.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys(), indexQuery.getAlias()));
                indexQuery.setLimitFrom(this.getLimitFrom());
                indexQuery.setLimitTo(this.getLimitTo());
                indexQuery.executeOn(this.getDataNode());
                indexQuery.setSubQuery(this.isSubQuery());
                indexQuery.having(OptimizerUtils.copyFilter(this.getHavingFilter(), indexQuery.getAlias()));
                indexQuery.valueQuery(FilterUtils.and(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter(),
                    indexQuery.getAlias()), OptimizerUtils.copyFilter(this.getResultFilter(), indexQuery.getAlias())));
                indexQuery.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter(),
                    indexQuery.getAlias()));
                indexQuery.build();
                return indexQuery;
            } else {
                indexQuery.alias(indexUsed.getNameWithOutDot());
                indexQuery.keyQuery(OptimizerUtils.copyFilter(this.getKeyFilter(), indexQuery.getAlias()));
                indexQuery.valueQuery(OptimizerUtils.copyFilter(this.getIndexQueryValueFilter(), indexQuery.getAlias()));

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
                keyQuery.valueQuery(OptimizerUtils.copyFilter(this.getResultFilter(), keyQuery.getAlias()));

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

                String tableName = this.getTableName();
                if (this.getAlias() != null) {
                    tableName = this.getAlias();
                }

                join.select(OptimizerUtils.copySelectables(this.getColumnsSelected(), tableName));
                join.setOrderBys(OptimizerUtils.copyOrderBys(this.getOrderBys(), tableName));
                join.setGroupBys(OptimizerUtils.copyOrderBys(this.getGroupBys(), tableName));
                join.setUsedForIndexJoinPK(true);
                join.setLimitFrom(this.getLimitFrom());
                join.setLimitTo(this.getLimitTo());
                join.executeOn(this.getDataNode());
                join.setSubQuery(this.isSubQuery());
                // 回表是IndexNestedLoop
                join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
                join.setAlias(this.getAlias());
                join.setSubAlias(this.getSubAlias());
                join.having(OptimizerUtils.copyFilter(this.getHavingFilter(), tableName));
                join.setOtherJoinOnFilter(OptimizerUtils.copyFilter(this.getOtherJoinOnFilter(), tableName));
                join.build();
                return join;
            }
        }
    }

    @Override
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

    @Override
    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    @Override
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

    public TableNode setIndexUsed(IndexMeta indexUsed) {
        this.indexUsed = indexUsed;
        return this;
    }

    // ============= insert/update/delete/put==================

    public InsertNode insert(List<ISelectable> columns, List<Object> values) {
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

    public InsertNode insert(String columns[], Object values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Object> valueList = new ArrayList<Object>(Arrays.asList(values));
        return this.insert(cs, valueList);
    }

    public PutNode put(List<ISelectable> columns, List<Object> values) {
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
        return put(StringUtils.split(columns, ' '), values);
    }

    public PutNode put(String columns[], Object values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Object> valueList = new ArrayList<Object>(Arrays.asList(values));
        return put(cs, valueList);
    }

    public UpdateNode update(List<ISelectable> columns, List<Object> values) {

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

    public UpdateNode update(String columns, Object values[]) {
        return update(columns.split(" "), values);
    }

    public UpdateNode update(String columns[], Object values[]) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : columns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Object> valueList = new ArrayList<Object>(Arrays.asList(values));
        return update(cs, valueList);
    }

    public DeleteNode delete() {
        DeleteNode delete = new DeleteNode(this);
        return delete;
    }

    // =============== copy =============
    @Override
    public TableNode copy() {
        TableNode newTableNode = new TableNode(null);
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    @Override
    protected void copySelfTo(QueryTreeNode to) {
        super.copySelfTo(to);
        TableNode toTable = (TableNode) to;
        toTable.setFullTableScan(this.isFullTableScan());
        toTable.setIndexQueryValueFilter((IFilter) (indexQueryValueFilter == null ? null : indexQueryValueFilter.copy()));
        toTable.tableName = this.tableName;
        toTable.actualTableName = this.actualTableName;
        toTable.setTableMeta(this.getTableMeta());
        toTable.useIndex(this.getIndexUsed());
    }

    @Override
    public TableNode deepCopy() {
        TableNode newTableNode = new TableNode(null);
        this.deepCopySelfTo(newTableNode);
        return newTableNode;
    }

    @Override
    protected void deepCopySelfTo(QueryTreeNode to) {
        super.deepCopySelfTo(to);
        TableNode toTable = (TableNode) to;
        toTable.setFullTableScan(this.isFullTableScan());
        toTable.setIndexQueryValueFilter((IFilter) (indexQueryValueFilter == null ? null : indexQueryValueFilter.copy()));
        toTable.tableName = this.tableName;
        toTable.actualTableName = this.actualTableName;
        toTable.setTableMeta(this.getTableMeta());
        toTable.useIndex(this.getIndexUsed());
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

    @Override
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
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter(), inden + 2), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getResultFilter(), inden + 2), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter(), inden + 2), tabContent);
        appendField(sb,
            "indexQueryValueFilter",
            printFilterString(this.getIndexQueryValueFilter(), inden + 2),
            tabContent);
        appendField(sb, "otherJoinOnFilter", printFilterString(this.getOtherJoinOnFilter(), inden + 2), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter(), inden + 2), tabContent);
        if (this.getIndexUsed() != null) {
            appendField(sb, "indexUsed", "\n" + this.getIndexUsed().toStringWithInden(inden + 2), tabContent);
        }
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
