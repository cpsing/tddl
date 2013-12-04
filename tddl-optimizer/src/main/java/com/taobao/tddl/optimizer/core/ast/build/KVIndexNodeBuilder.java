package com.taobao.tddl.optimizer.core.ast.build;

import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * @since 5.1.0
 */
public class KVIndexNodeBuilder extends QueryTreeNodeBuilder {

    public KVIndexNodeBuilder(KVIndexNode node){
        this.setNode(node);
    }

    public KVIndexNode getNode() {
        return (KVIndexNode) super.getNode();
    }

    public void build() {
        this.buildIndex();
        this.buildSelected();
        this.buildGroupBy();
        this.buildOrderBy();
        this.buildWhere();
    }

    /**
     * 構建列信息
     * 
     * @param indexNode
     */
    public void buildSelected() {
        this.getNode().getImplicitSelectable().clear();
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
            if (IColumn.STAR.equals(selected.getColumnName())) {
                delete.add(selected);
            }
        }

        if (!delete.isEmpty()) {
            this.getNode().getColumnsSelected().removeAll(delete);

            // 遇到*就把所有列再添加一遍
            // select *,id这样的语法最后会有两个id列，mysql是这样的
            for (ColumnMeta cm : this.getNode().getIndex().getKeyColumns()) {
                this.getNode()
                    .getColumnsSelected()
                    .add(OptimizerUtils.columnMetaToIColumn(cm, this.getNode().getIndexName()));
            }

            for (ColumnMeta cm : this.getNode().getIndex().getValueColumns()) {
                this.getNode()
                    .getColumnsSelected()
                    .add(OptimizerUtils.columnMetaToIColumn(cm, this.getNode().getIndexName()));
            }
        }

        for (int i = 0; i < getNode().getColumnsSelected().size(); i++) {
            this.getNode().getColumnsSelected().set(i, this.buildSelectable(getNode().getColumnsSelected().get(i)));
        }
    }

    /**
     * 构建索引信息
     * 
     * @param getNode ()
     */
    public void buildIndex() {
        String kvIndexName = getNode().getKvIndexName();
        if (kvIndexName != null) {
            IndexMeta index = OptimizerContext.getContext().getIndexManager().getIndexByName(kvIndexName);
            if (index == null) {
                throw new OptimizerException("index :" + kvIndexName + " is not found");
            }

            getNode().setIndex(index);
            getNode().setTableMeta(OptimizerContext.getContext().getSchemaManager().getTable(index.getTableName()));
        } else if (getNode().getIndex() == null) {
            throw new OptimizerException("index is null");
        }

    }

    public ISelectable getSelectableFromChild(ISelectable c) {
        if (c.getTableName() != null) {
            if ((!c.getTableName().equals(this.getNode().getIndexName()))
                && (!c.getTableName().equals(this.getNode().getAlias()))) {
                // 貌似不应该再去匹配tableName，不然一旦一张表的两个索引进行join时，查询字段就会串了
                // && (!c.getTableName().equals(this.getNode().getTableName()))
                return null;
            }
        }
        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        if (c instanceof IFunction) {
            c.setTableName(this.getNode().getIndexName());
            return c;
        }

        return this.getSelectableFromChild(c.getColumnName());
    }

    private ISelectable getSelectableFromChild(String columnName) {
        ColumnMeta cm = this.getNode().getIndex().getColumnMeta(columnName);
        if (cm == null) {
            return null;
        }

        return OptimizerUtils.columnMetaToIColumn(cm, getNode().getIndexName());
    }
}
