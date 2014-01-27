package com.taobao.tddl.optimizer.core.ast.build;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * @since 5.0.0
 */
public class TableNodeBuilder extends QueryTreeNodeBuilder {

    public TableNodeBuilder(TableNode tableNode){
        this.setNode(tableNode);
    }

    public void build() {
        this.buildTableMeta();
        this.buildSelected();
        this.buildWhere();
        this.buildGroupBy();
        this.buildOrderBy();
        this.buildHaving();
        this.buildExistAggregate();
    }

    public TableNode getNode() {
        return (TableNode) super.getNode();
    }

    /**
     * 构建TableMeta
     * 
     * @param getNode ()
     */
    public void buildTableMeta() {
        String tableName = getNode().getTableName();
        if (tableName == null) {
            throw new IllegalArgumentException("tableName is null");
        }

        TableMeta ts = OptimizerContext.getContext().getSchemaManager().getTable(tableName);
        getNode().setTableMeta(ts);
    }

    /**
     * 构建列信息
     */
    public void buildSelected() {
        buildSelectedFromSelectableObject();
    }

    private void buildSelectedFromSelectableObject() {
        if (this.getNode().getColumnsSelected().isEmpty()) {
            this.getNode()
                .getColumnsSelected()
                .add(ASTNodeFactory.getInstance().createColumn().setColumnName(IColumn.STAR));
        }

        // 如果有 * ，最后需要把*删掉
        List<Integer> delete = new LinkedList();

        int index = 0;
        for (ISelectable selected : getNode().getColumnsSelected()) {
            if (IColumn.STAR.equals(selected.getColumnName())) {
                delete.add(index);
                break;
            }
        }

        // 把星号替换成列，允许存在多个*，或则多个相同字段的列
        if (!delete.isEmpty()) {
            List<ISelectable> columnsWithOutStar = new ArrayList();
            for (int i = 0; i < this.getNode().getColumnsSelected().size(); i++) {
                if (IColumn.STAR.equals(this.getNode().getColumnsSelected().get(i).getColumnName())) {
                    for (ColumnMeta cm : this.getNode().getTableMeta().getAllColumns()) {
                        columnsWithOutStar.add(ASTNodeFactory.getInstance()
                            .createColumn()
                            .setColumnName(cm.getName())
                            .setDataType(cm.getDataType()));
                    }
                } else {
                    columnsWithOutStar.add(this.getNode().getColumnsSelected().get(i));
                }
            }
            this.getNode().select(columnsWithOutStar);
        }

        for (int i = 0; i < getNode().getColumnsSelected().size(); i++) {
            getNode().getColumnsSelected().set(i, this.buildSelectable(getNode().getColumnsSelected().get(i)));
        }

    }

    public ISelectable getSelectableFromChild(ISelectable c) {
        // 如果存在表名，则进行强校验，比如字段为A.ID，否则直接进行ID名字匹配
        if (c.getTableName() != null && !c.getTableName().equals(this.getNode().getTableName())
            && !c.getTableName().equals(this.getNode().getAlias())) {
            return null;
        }

        if (IColumn.STAR.equals(c.getColumnName())) {
            return c;
        }

        if (c instanceof IFunction) {
            c.setTableName(this.getNode().getTableName());
            return c;
        }

        return this.getSelectableFromChild(c.getColumnName());
    }

    public ISelectable getSelectableFromChild(String name) {
        ColumnMeta res = getNode().getTableMeta().getColumn(name);
        if (res == null) {
            return null;
        }

        return OptimizerUtils.columnMetaToIColumn(res, getNode().getTableName());
    }
}
