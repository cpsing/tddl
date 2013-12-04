package com.taobao.tddl.optimizer.core.ast.build;

import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.1.0
 */
public class QueryNodeBuilder extends QueryTreeNodeBuilder {

    public QueryNodeBuilder(QueryNode queryNode){
        this.setNode(queryNode);
    }

    public QueryNode getNode() {
        return (QueryNode) super.getNode();
    }

    public void build() {
        for (ASTNode sub : this.getNode().getChildren()) {
            sub.build();
        }

        if (!(this.getNode().getChild() instanceof QueryTreeNode)) {// 嵌套子类
            return;
        }

        this.buildAlias();
        this.buildSelected();

        this.buildWhere();
        this.buildGroupBy();
        this.buildOrderBy();

        if (this.getNode().getDataNode() == null) {
            this.getNode().executeOn(this.getNode().getChild().getDataNode());
        }
    }

    private void buildAlias() {
        if (this.getNode().getAlias() == null) {
            this.getNode().alias(this.getNode().getChild().getAlias());
        }
    }

    public void buildSelected() {
        this.getNode().getImplicitSelectable().clear();

        // 如果没有给Merge指定select列，则从child中继承select列
        if (this.getNode().getColumnsSelected() == null || this.getNode().getColumnsSelected().isEmpty()) {
            List<ISelectable> childSelected = ((QueryTreeNode) this.getNode().getChildren().get(0)).getColumnsSelectedForParent();
            this.getNode().select(childSelected);
        }

        if (this.getNode().getChildren() != null) {
            for (int i = 0; i < this.getNode().getChildren().size(); i++) {
                QueryTreeNode child = (QueryTreeNode) this.getNode().getChildren().get(i);
                // merge的子节点需要把临时列也选上
                if (child.getImplicitSelectable() != null && !child.getImplicitSelectable().isEmpty()) {
                    child.select(child.getColumnsRefered());// 返回所有子列
                    child.build();
                }
            }
        }

        buildSelectedFromSelectableObject();
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

        if (!delete.isEmpty()) {
            this.getNode().getColumnsSelected().removeAll(delete);
        }

        for (ISelectable selected : delete) {
            // 遇到*就把所有列再添加一遍
            // select *,id这样的语法最后会有两个id列，mysql是这样的
            QueryTreeNode child = (QueryTreeNode) this.getNode().getChild();

            for (ISelectable selectedFromChild : child.getColumnsSelected()) {
                if (selected.getTableName() != null
                    && !selected.getTableName().equals(selectedFromChild.getTableName())) {
                    break;
                }

                IColumn newS = ASTNodeFactory.getInstance().createColumn();

                if (child.getAlias() != null) {// sub alias
                    newS.setTableName(child.getAlias());
                } else {
                    newS.setTableName(selectedFromChild.getTableName());
                }

                if (selectedFromChild.getAlias() == null) {
                    newS.setColumnName(selectedFromChild.getColumnName());
                } else {
                    newS.setColumnName(selectedFromChild.getAlias());
                }

                getNode().getColumnsSelected().add(newS);// 允许多列
            }
        }

        for (int i = 0; i < getNode().getColumnsSelected().size(); i++) {
            getNode().getColumnsSelected().set(i, this.buildSelectable(getNode().getColumnsSelected().get(i)));
        }

    }

    public ISelectable getSelectableFromChild(ISelectable c) {
        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        if (c instanceof IFunction) {
            return c;
        }

        if (c instanceof IBooleanFilter && ((IBooleanFilter) c).getOperation().equals(OPERATION.CONSTANT)) {
            return c;
        }

        return this.getColumnFromOtherNode(c);
    }

    public ISelectable getColumnFromOtherNode(ISelectable c) {
        ISelectable res = null;
        QueryTreeNode child = this.getNode().getChild();
        for (ISelectable selected : child.getColumnsSelected()) {
            boolean isThis = false;
            if (c.getTableName() != null) {
                if (!(c.getTableName().equals(this.getNode().getAlias()))) {
                    continue;
                }
            }

            if (IColumn.STAR.equals(c.getColumnName())) {
                return c;
            }

            // 若列别名存在，只比较别名
            isThis = c.isSameName(selected);

            if (isThis) {
                res = selected;
                break;
            }
        }

        if (res == null) {
            return res;
        }

        if (c instanceof IColumn) {
            // 如果是子表的结构，比如Join/Merge的子节点，字段的名字直接使用别名
            if (this.getNode().getAlias() != null) {
                c.setTableName(this.getNode().getAlias());
            } else {
                c.setTableName(res.getTableName());
            }
        }

        return c;
    }
}
