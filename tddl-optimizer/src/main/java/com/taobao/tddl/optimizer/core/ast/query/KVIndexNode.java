package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.KVIndexNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class KVIndexNode extends TableNode {

    private IndexMeta          index       = null;
    private String             kvIndexName = null;

    private KVIndexNodeBuilder builder;

    public KVIndexNode(String kvIndexName){
        this.kvIndexName = kvIndexName;
        builder = new KVIndexNodeBuilder(this);
    }

    public KVIndexNode keyQuery(IFilter f) {
        this.setNeedBuild(true);
        this.keyFilter = f;
        return this;
    }

    public KVIndexNode keyQuery(String f) {
        this.setNeedBuild(true);
        this.keyFilter = FilterUtils.createFilter(f);
        return this;
    }

    public KVIndexNode valueQuery(IFilter f) {
        this.setNeedBuild(true);
        this.resultFilter = f;
        return this;
    }

    public KVIndexNode valueQuery(String f) {
        this.setNeedBuild(true);
        this.resultFilter = FilterUtils.createFilter(f);
        return this;
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public IQuery toDataNodeExecutor() {
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        query.setAlias(this.getAlias());
        query.setColumns(this.getColumnsSelected());
        query.setConsistent(this.getConsistent());
        query.setGroupBys(this.getGroupBys());
        String tableName = null;
        if (this.getActualTableName() != null) {
            tableName = this.getActualTableName();
        } else if (this.getIndex() != null) {
            tableName = this.getIndex().getName();
        }

        query.setTableName(tableName);
        query.setIndexName(this.getIndex() == null ? null : this.getIndex().getName());
        query.setKeyFilter(this.getKeyFilter());
        query.setValueFilter(this.getResultFilter());
        query.setLimitFrom(this.getLimitFrom());
        query.setLimitTo(this.getLimitTo());
        query.setLockModel(this.getLockModel());
        query.setOrderBys(this.getOrderBys());
        query.setSubQuery(null);
        query.executeOn(this.getDataNode());
        query.setSql(this.getSql());
        query.setIsSubQuery(this.isSubQuery());
        query.setExtra(this.getExtra());
        query.having(this.getHavingFilter());
        return query;
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    // ================ setter / getter ===============

    public String getTableName() {
        return this.getIndex().getTableName();
    }

    public String getName() {
        if (this.getAlias() != null) {
            return this.getAlias();
        }

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
        return this.getIndexName();
    }

    public String toString(int inden) {

        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();

        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "Query from " + this.getIndexName() + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "Query from " + this.getIndexName());
        }
        appendField(sb, "actualTableName", this.getActualTableName(), tabContent);
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter()), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getResultFilter()), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter()), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter()), tabContent);
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
