package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.QueryNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * @author Dreamond 对于一个单个逻辑表的query，处理node.
 * @author whisper
 */
public class QueryNode extends QueryTreeNode {

    private QueryNodeBuilder builder;

    public QueryNode(QueryTreeNode child){
        this(child, null);
    }

    public QueryNode(QueryTreeNode child, IFilter filter){
        this.builder = new QueryNodeBuilder(this);
        this.whereFilter = filter;
        this.setChild(child);
        if (child != null) {
            child.setSubQuery(true);// 默认设置为subQuery
        }
    }

    public QueryTreeNode getChild() {
        if (this.getChildren().isEmpty()) {
            return null;
        }
        return (QueryTreeNode) this.getChildren().get(0);
    }

    public void setChild(QueryTreeNode child) {
        if (child == null) {
            return;
        }

        if (this.getChildren().isEmpty()) {
            this.getChildren().add(child);
        } else {
            this.getChildren().set(0, child);
        }

        setNeedBuild(true);
    }

    public List<ASTNode> getChildren() {
        if (super.getChildren() != null && super.getChildren().size() == 1) {
            if (super.getChildren().get(0) == null) {
                super.getChildren().remove(0);
            }
        }

        return super.getChildren();
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    public List getImplicitOrderBys() {
        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        } else {
            return this.getChild().getImplicitOrderBys();
        }
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public String getName() {
        return this.getAlias();
    }

    public IQueryTree toDataNodeExecutor() throws QueryException {
        IQuery query = ASTNodeFactory.getInstance().createQuery();
        query.setAlias(this.getAlias());
        query.setColumns(this.getColumnsSelected());
        query.setConsistent(this.getConsistent());
        query.setGroupBys(this.getGroupBys());

        query.setKeyFilter(this.getKeyFilter());
        query.setValueFilter(this.getResultFilter());

        query.setLimitFrom(this.getLimitFrom());
        query.setLimitTo(this.getLimitTo());
        query.setLockModel(this.getLockModel());
        query.setOrderBys(this.getOrderBys());
        query.setSubQuery(this.getChild().toDataNodeExecutor());
        query.executeOn(this.getDataNode());
        query.setSql(this.getSql());
        query.setIsSubQuery(this.isSubQuery());
        return query;
    }

    public QueryNode copy() {
        QueryNode newTableNode = new QueryNode((QueryTreeNode) this.getChild().copy());
        this.copySelfTo(newTableNode);
        newTableNode.setNeedBuild(false);
        return newTableNode;
    }

    public QueryNode deepCopy() {
        QueryNode newTableNode = new QueryNode((QueryTreeNode) this.getChild().deepCopy());
        this.deepCopySelfTo(newTableNode);
        newTableNode.setNeedBuild(false);
        return newTableNode;
    }

    public String toString(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        if (this.getAlias() != null) {
            appendln(sb, tabTittle + "SubQuery" + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + "SubQuery");
        }
        appendField(sb, "keyFilter", printFilterString(this.getKeyFilter(), inden + 2), tabContent);
        appendField(sb, "resultFilter", printFilterString(this.getResultFilter(), inden + 2), tabContent);
        appendField(sb, "whereFilter", printFilterString(this.getWhereFilter(), inden + 2), tabContent);
        appendField(sb, "otherJoinOnFilter", printFilterString(this.getOtherJoinOnFilter(), inden + 2), tabContent);
        appendField(sb, "having", printFilterString(this.getHavingFilter(), inden + 2), tabContent);

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

        if (this.getChild() != null) {
            appendln(sb, tabContent + "from:");
            sb.append(this.getChild().toString(inden + 2));
        }
        return sb.toString();
    }

}
