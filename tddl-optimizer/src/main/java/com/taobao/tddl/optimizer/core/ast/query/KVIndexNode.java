package com.taobao.tddl.optimizer.core.ast.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.KVIndexNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class KVIndexNode extends TableNode {

    private IndexMeta          index       = null;
    private String             kvIndexName = null;

    private KVIndexNodeBuilder builder;

    public KVIndexNode(String tableName){
        super(tableName);

        builder = new KVIndexNodeBuilder(this);
    }

    public KVIndexNode keyQuery(IFilter f) {
        this.setNeedBuild(true);
        this.keyFilter = f;
        return this;
    }

    public KVIndexNode valueQuery(IFilter f) {
        this.setNeedBuild(true);
        this.resultFilter = f;
        return this;
    }

    public IQuery toDataNodeExecutor() {
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        // query.setAlias(this.getAlias());
        // query.setColumns(this.getColumnsSelected());
        // query.setConsistent(this.getConsistent());
        // query.setGroupBys(this.getGroupBys());
        // String dbName = null;
        // if (this.getDbName() != null) {
        // dbName = this.getDbName();
        // } else if (this.getIndex() != null) {
        // dbName = this.getIndex().getName();
        // }
        // query.setActualTableName(dbName);
        // query.setIndexName(this.getIndex() == null ? null :
        // this.getIndex().getName());
        // query.setKeyFilter(this.getKeyFilter());
        // query.setValueFilter(this.getResultFilter());
        // query.setLimitFrom(this.getLimitFrom());
        // query.setLimitTo(this.getLimitTo());
        // query.setLockModel(this.getLockModel());
        // query.setOrderBy(this.getOrderBys());
        // query.setSubQuery(null);
        // query.executeOn(this.getDataNode());
        // query.setSql(this.getSql());
        // query.setIsSubQuery(this.isSubQuery());
        // query.setExtra(this.getExtra());
        // query.having(this.getHavingFilter());
        return query;
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    // public KVIndexNodeBuilder getBuilder() {
    // TODO return this.builder;
    // return null;
    // }

    public InsertNode insert(List<ISelectable> columns, List<Comparable> values) {
        return super.insert(OptimizerUtils.copySelectables(columns), values);
    }

    public UpdateNode update(List<ISelectable> columns, List<Comparable> values) {
        return super.update(OptimizerUtils.copySelectables(columns), values);
    }

    public PutNode put(List<ISelectable> columns, List<Comparable> values) {
        return super.put(OptimizerUtils.copySelectables(columns), values);
    }

    // ================ setter / getter ===============

    public String getTableName() {
        return this.getIndex().getTableName();
    }

    public String getName() {
        if (this.getAlias() != null) return this.getAlias();
        return this.getIndexName();
    }

    public IndexMeta getIndexUsed() {
        return this.getIndex();
    }

    public String getIndexName() {
        return this.getKvIndexName();
    }

    public IndexMeta getIndex() {
        return index;
    }

    public void setIndex(IndexMeta index) {
        this.index = index;
    }

    public String getKvIndexName() {
        return kvIndexName;
    }

    public void setKvIndexName(String kvIndexName) {
        this.kvIndexName = kvIndexName;
    }

    public void setIndexQueryValueFilter(IFilter indexValueFilter) {
        super.setIndexQueryValueFilter(OptimizerUtils.copyFilter(indexValueFilter));
    }

    public void setOrderBys(List<IOrderBy> explicitOrderBys) {
        super.setOrderBys(OptimizerUtils.copyOrderBys(explicitOrderBys));
    }

    public QueryTreeNode query(IFilter whereFilter) {
        return super.query(OptimizerUtils.copyFilter(whereFilter));
    }

    public QueryTreeNode setGroupBys(List<IOrderBy> groups) {
        return super.setGroupBys(OptimizerUtils.copyOrderBys(groups));
    }

    public QueryTreeNode select(List<ISelectable> columnSelected) {
        return super.select(OptimizerUtils.copySelectables(columnSelected));
    }

    public void addResultFilter(IBooleanFilter filter) {
        super.addResultFilter((IBooleanFilter) OptimizerUtils.copyFilter(filter));
    }

    public void setKeyFilter(IFilter f) {
        super.setKeyFilter(OptimizerUtils.copyFilter(f));
    }

    public void setResultFilter(IFilter f) {
        super.setResultFilter(OptimizerUtils.copyFilter(f));
    }

    public void setImplicitSelectable(List<ISelectable> implicitSelectable) {
        super.setImplicitSelectable(OptimizerUtils.copySelectables(implicitSelectable));
    }

    public QueryTreeNode orderBy(ISelectable c, boolean b) {
        return super.orderBy(c.copy(), b);
    }

    public KVIndexNode copy() {
        KVIndexNode newTableNode = new KVIndexNode(this.getIndexName());
        this.copySelfTo(newTableNode);
        return newTableNode;
    }

    protected void copySelfTo(QueryTreeNode to) {
        super.copySelfTo(to);
        KVIndexNode toTable = (KVIndexNode) to;
        toTable.index = index;
        toTable.kvIndexName = kvIndexName;
        toTable.setTableMeta(this.getTableMeta());
        toTable.setNeedBuild(false);
    }

    public KVIndexNode deepCopy() {
        KVIndexNode newTableNode = new KVIndexNode(this.getIndexName());
        this.deepCopySelfTo(newTableNode);
        return newTableNode;
    }

    protected void deepCopySelfTo(QueryTreeNode to) {
        super.deepCopySelfTo(to);
        KVIndexNode toTable = (KVIndexNode) to;
        toTable.index = index;
        toTable.kvIndexName = kvIndexName;
        toTable.setTableMeta(this.getTableMeta());
        toTable.setNeedBuild(false);
    }

    public void assignment(Map<Integer, ParameterContext> parameterSettings) {
        super.assignment(parameterSettings);
    }

    public List<IOrderBy> getImplicitOrderBys() {
        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        }

        List<IOrderBy> implicitOrdersCandidate = OptimizerUtils.getOrderBy(index);
        List<IOrderBy> implicitOrders = new ArrayList();
        for (int i = 0; i < implicitOrdersCandidate.size(); i++) {
            implicitOrdersCandidate.get(i).setTableName(this.getIndexName());
            if (this.getColumnsSelected().contains(implicitOrdersCandidate.get(i).getColumn())) {
                implicitOrders.add(implicitOrdersCandidate.get(i));
            } else {
                break;
            }
        }

        return implicitOrders;

    }

    public String getSchemaName() {
        if (super.getSchemaName() == null) {
            return this.getIndexName();
        }

        return super.getSchemaName();
    }

    public List<ISelectable> getColumnsRefered() {
        throw new NotSupportException("KVIndexNode的getColumnsRefered不应被调用");

    }

    public void selectAColumn(ISelectable s) {
        if (s.getTableName() != null) {
            if (!(s.getTableName().equals(this.getTableName()) || s.getTableName().equals(this.getAlias()))) {
                return;
            }
        }
        if (this.columnsSelected == null) {
            this.columnsSelected = new ArrayList();
        }
        if (!this.columnsSelected.contains(s)) {
            columnsSelected.add(s);
        }
    }
}
