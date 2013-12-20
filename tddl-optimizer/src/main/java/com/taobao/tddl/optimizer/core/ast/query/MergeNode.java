package com.taobao.tddl.optimizer.core.ast.query;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.printFilterString;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.build.MergeNodeBuilder;
import com.taobao.tddl.optimizer.core.ast.build.QueryTreeNodeBuilder;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class MergeNode extends QueryTreeNode {

    private MergeNodeBuilder builder;
    private boolean          sharded = true;
    private boolean          union   = false;

    public MergeNode(){
        this.builder = new MergeNodeBuilder(this);
    }

    public void build() {
        if (this.isNeedBuild()) {
            this.builder.build();
        }

        setNeedBuild(false);
    }

    public MergeNode merge(ASTNode o) {
        this.addChild(o);
        return this;
    }

    public MergeNode merge(List<ASTNode> os) {
        this.getChildren().addAll(os);
        return this;
    }

    public IQueryTree toDataNodeExecutor() throws QueryException {
        IMerge merge = ASTNodeFactory.getInstance().createMerge();
        merge.setLimitFrom(this.getLimitFrom());
        merge.setLimitTo(this.getLimitTo());
        merge.executeOn(this.getDataNode());
        merge.setColumns(this.getColumnsSelected());
        merge.setAlias(this.getAlias());
        merge.setIsSubQuery(this.isSubQuery());
        merge.setUnion(this.isUnion());
        for (ASTNode subQuery : this.getChildren()) {
            merge.addSubNode(subQuery.toDataNodeExecutor());
        }
        merge.setOrderBys(this.getOrderBys()).setLimitFrom(this.getLimitFrom()).setLimitTo(this.getLimitTo());
        merge.setGroupBys(this.getGroupBys());
        merge.setSharded(this.isSharded());
        merge.having(this.getHavingFilter());
        return merge;
    }

    public List getImplicitOrderBys() {
        if (!(this.getChild() instanceof QueryTreeNode)) {
            return new ArrayList<IOrderBy>(0);
        }

        List<IOrderBy> orderByCombineWithGroupBy = getOrderByCombineWithGroupBy();
        if (orderByCombineWithGroupBy != null) {
            return orderByCombineWithGroupBy;
        } else {
            return new ArrayList<IOrderBy>(0);
        }
    }

    public QueryTreeNodeBuilder getBuilder() {
        return builder;
    }

    public String getName() {
        return this.getAlias();
    }

    public void setSharded(boolean b) {
        this.sharded = b;
    }

    public boolean isSharded() {
        return this.sharded;
    }

    public boolean isUnion() {
        return union;
    }

    public void setUnion(boolean union) {
        this.union = union;
    }

    public MergeNode copy() {
        MergeNode newMergeNode = new MergeNode();
        this.copySelfTo(newMergeNode);
        newMergeNode.setSharded(sharded);
        newMergeNode.setUnion(union);
        return newMergeNode;
    }

    public MergeNode deepCopy() {
        MergeNode newMergeNode = new MergeNode();
        this.deepCopySelfTo(newMergeNode);
        newMergeNode.setSharded(sharded);
        newMergeNode.setUnion(union);
        return newMergeNode;
    }

    public String toString(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        if (this.getAlias() != null) {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge") + " as " + this.getAlias());
        } else {
            appendln(sb, tabTittle + (this.isUnion() ? "Union" : "Merge"));
        }

        appendField(sb, "resultFilter", printFilterString(this.getResultFilter(), inden + 2), tabContent);
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
        appendField(sb, "columns", this.getColumnsSelected(), tabContent);
        appendField(sb, "groupBys", this.getGroupBys(), tabContent);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        appendln(sb, tabContent + "subQueries");
        for (Object s : this.getChildren()) {
            ASTNode node = (ASTNode) s;
            sb.append(node.toString(inden + 2));
        }

        return sb.toString();
    }

}
