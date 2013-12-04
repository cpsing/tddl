package com.taobao.tddl.optimizer.core.ast.build;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.1.0
 */
public class MergeNodeBuilder extends QueryTreeNodeBuilder {

    public MergeNodeBuilder(MergeNode mergeNode){
        this.setNode(mergeNode);
    }

    public MergeNode getNode() {
        return (MergeNode) super.getNode();
    }

    public void build() {
        for (ASTNode sub : this.getNode().getChildren()) {
            sub.build();
        }
        if (!(this.getNode().getChild() instanceof QueryTreeNode)) {
            return;
        }

        this.buildAlias();
        this.buildSelected();
        this.buildWhere();
        this.buildGroupBy();
        this.buildOrderBy();
        this.buildHaving();
        this.buildLimit();
        this.pushSelect();
        this.pushAggregateFunction();
    }

    /**
     * max(id)+min(id)，要把聚合函数推到子节点去
     */
    private void pushAggregateFunction() {
        List<IFunction> aggregateInScalar = new ArrayList();
        for (ISelectable s : this.getNode().getColumnsSelected()) {
            if (s instanceof IFunction) {
                if (IFunction.FunctionType.Aggregate.equals(((IFunction) s).getFunctionType())) {
                    continue;
                }

                aggregateInScalar.addAll(this.findAggregateFunctionsInScalar((IFunction) s));
            }
        }

        List<ISelectable> toRemove = new ArrayList();
        for (ISelectable s : ((QueryTreeNode) this.getNode().getChild()).getColumnsSelected()) {
            if (s instanceof IFunction && IFunction.FunctionType.Scalar.equals(((IFunction) s).getFunctionType())) {
                if (!this.findAggregateFunctionsInScalar((IFunction) s).isEmpty()) {
                    toRemove.add(s);
                }
            }
        }

        for (ASTNode child : this.getNode().getChildren()) {
            ((QueryTreeNode) child).getColumnsSelected().removeAll(toRemove);// 干掉查询的min(id)+max(id)函数
            for (ISelectable f : aggregateInScalar) {
                // 只添加min(id) ,max(id)的独立函数
                ((QueryTreeNode) child).addColumnsSelected(f);
            }
        }

    }

    private List<IFunction> findAggregateFunctionsInScalar(IFunction s) {
        List<IFunction> res = new ArrayList();
        this.findAggregateFunctionsInScalar(s, res);
        return res;
    }

    private void findAggregateFunctionsInScalar(IFunction s, List<IFunction> res) {
        if (IFunction.FunctionType.Aggregate.equals(s.getFunctionType())) {
            res.add(s);
        }

        for (Object arg : s.getArgs()) {
            if (arg instanceof IFunction) {
                this.findAggregateFunctionsInScalar((IFunction) arg, res);
            }
        }
    }

    public void buildHaving() {
        if (this.getNode().getChild() instanceof QueryTreeNode) {
            // 干掉子节点查询的having条件，转移到父节点中
            this.getNode().having(((QueryTreeNode) this.getNode().getChild()).getHavingFilter());
            for (ASTNode child : this.getNode().getChildren()) {
                if (child instanceof QueryTreeNode) {
                    ((QueryTreeNode) child).having("");
                }
            }
        }

        replaceAliasInFilter(this.getNode().getHavingFilter(), ((QueryTreeNode) this.getNode().getChild()).getAlias());
    }

    private void replaceAliasInFilter(Object filter, String alias) {
        if (filter instanceof IFunction) {
            for (Object sub : ((IFunction) filter).getArgs()) {
                this.replaceAliasInFilter(sub, alias);
            }
        }

        if (filter instanceof ISelectable) {
            if (alias != null) {
                ((ISelectable) filter).setTableName(alias);
            }

            if (((ISelectable) filter).getAlias() != null) {
                ((ISelectable) filter).setColumnName(((ISelectable) filter).getAlias());
                ((ISelectable) filter).setAlias(null);
            }
        }

    }

    private void pushSelect() {
        // 不存在临时列，无需下推
        if (this.getNode().getGroupBys().isEmpty() && this.getNode().getOrderBys().isEmpty()) {
            return;
        }

        // 将orderBy/groupBy下推
        for (ASTNode child : this.getNode().getChildren()) {
            List<IOrderBy> orderByAndGroupBy = new ArrayList(((QueryTreeNode) child).getOrderBys());
            orderByAndGroupBy.addAll(((QueryTreeNode) child).getGroupBys());
            for (IOrderBy order : orderByAndGroupBy) {
                ((QueryTreeNode) child).addColumnsSelected(order.getColumn());
            }
        }

    }

    private void buildLimit() {
        // 将子节点的limit条件转移到父节点
        this.getNode().setLimitFrom(((QueryTreeNode) this.getNode().getChild()).getLimitFrom());
        this.getNode().setLimitTo(((QueryTreeNode) this.getNode().getChild()).getLimitTo());

        for (ASTNode s : this.getNode().getChildren()) {
            ((QueryTreeNode) s).setLimitFrom(null);
            ((QueryTreeNode) s).setLimitTo(null);
        }
    }

    public void buildOrderBy() {
        // 如果merge本身没指定order by，则继承子节点的order by
        if (this.getNode().getOrderBys() == null || this.getNode().getOrderBys().isEmpty()) {
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
            if (child.getOrderBys() != null) {
                for (IOrderBy o : child.getOrderBys()) {
                    ISelectable sc = o.getColumn().copy();
                    if (o.getColumn().getAlias() != null) {
                        sc.setColumnName(o.getColumn().getAlias());
                    }
                    if (child.getAlias() != null) {
                        sc.setTableName(child.getAlias());
                    }
                    this.getNode().orderBy(sc, o.getDirection());
                }
            }
        }
    }

    public void buildGroupBy() {
        // 如果merge本身没指定group by，则继承子节点的group by
        if (this.getNode().getGroupBys() == null || this.getNode().getGroupBys().isEmpty()) {
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
            if (child.getGroupBys() != null) {
                for (IOrderBy s : child.getGroupBys()) {
                    IOrderBy sc = s.copy();
                    if (s.getAlias() != null) {
                        sc.setColumnName(s.getAlias());
                    }
                    if (child.getAlias() != null) {
                        sc.setTableName(child.getAlias());
                    }
                    this.getNode().groupBy(sc);
                }
            }
        }
    }

    private void buildAlias() {
        if (this.getNode().getAlias() == null) {
            this.getNode().alias(((QueryTreeNode) this.getNode().getChild()).getAlias());
        }
    }

    /**
     * 构建列信息
     * 
     * @param indexNode
     */
    public void buildSelected() {
        this.getNode().getImplicitSelectable().clear();

        // 如果没有给Merge指定select列，则从child中继承select列
        if (this.getNode().getColumnsSelected() == null || this.getNode().getColumnsSelected().isEmpty()) {
            List<ISelectable> childSelected = ((QueryTreeNode) this.getNode().getChildren().get(0)).getColumnsSelectedForParent();
            this.getNode().select(childSelected);
        }

        buildSelectedFromSelectableObject();
        this.buildFunction();
    }

    private void buildSelectedFromSelectableObject() {
        if (this.getNode().getColumnsSelected().isEmpty()) {
            this.getNode()
                .getColumnsSelected()
                .add(ASTNodeFactory.getInstance().createColumn().setColumnName(IColumn.STAR));
        }
        // 如果有 * ，最后需要把*删掉
        List<ISelectable> delete = new LinkedList();

        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (selected.getColumnName().equals(IColumn.STAR)) {
                delete.add(selected);
            }
        }
        if (!delete.isEmpty()) this.getNode().getColumnsSelected().removeAll(delete);

        for (ISelectable selected : delete) {
            // 遇到*就把所有列再添加一遍
            // select *,id这样的语法最后会有两个id列，mysql是这样的
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();

            for (ISelectable selectedFromChild : child.getColumnsSelected()) {
                if (selected.getTableName() != null) {
                    if (!selected.getTableName().equals(selectedFromChild.getTableName())) {
                        break;
                    }
                }

                IColumn newS = ASTNodeFactory.getInstance().createColumn();

                if (child.getAlias() != null) {
                    newS.setTableName(child.getAlias());
                } else {
                    newS.setTableName(selectedFromChild.getTableName());
                }

                if (selectedFromChild.getAlias() == null) {
                    newS.setColumnName(selectedFromChild.getColumnName());
                } else {
                    newS.setColumnName(selectedFromChild.getAlias());
                }

                getNode().getColumnsSelected().add(newS);
            }
        }

    }

    public ISelectable getSelectableFromChild(ISelectable c) {
        QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();
        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        if (c instanceof IFunction) {
            return c;
        }

        ISelectable s = this.getColumnFromOtherNode(c, child);
        if (s == null) {
            for (int i = 0; i < this.getNode().getChildren().size(); i++) {
                QueryTreeNode sub = (QueryTreeNode) this.getNode().getChildren().get(i);
                sub.addColumnsSelected(c);
            }
            s = this.getColumnFromOtherNode(c, child);
        }
        return s;
    }

    public void buildFunction(IFunction f) {
        for (Object arg : f.getArgs()) {
            if (arg instanceof IFunction) {
                this.buildSelectable((ISelectable) arg);
            } else if (arg instanceof ISelectable) {
                if (((ISelectable) arg).isDistinct()) {// 比如count(distinct id)
                    this.buildSelectable((ISelectable) arg);
                }
            }
        }
    }

}
