package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class UpdateNode extends DMLNode<UpdateNode> {

    public UpdateNode(TableNode table){
        super(table);
    }

    public UpdateNode setUpdateColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getUpdateColumns() {
        return this.columns;
    }

    public UpdateNode setUpdateValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public List<Object> getUpdateValues() {
        return this.values;
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor() throws QueryException {
        IUpdate update = ASTNodeFactory.getInstance().createUpdate();
        for (ISelectable updateColumn : this.getColumns()) {
            if (this.getNode().getTableMeta().getPrimaryIndex().getPartitionColumns().contains(updateColumn)) {
                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是分库键，不允许修改");
            }

            if (this.getNode().getTableMeta().getPrimaryKeyMap().containsKey(updateColumn.getColumnName())) {
                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是主键，不允许修改");
            }
        }

        update.setConsistent(true);
        update.executeOn(this.getNode().getDataNode());
        update.setQueryTree(this.getNode().toDataNodeExecutor());
        update.setUpdateColumns(this.getUpdateColumns());
        update.setUpdateValues(values);
        if (this.getNode().getActualTableName() != null) {
            update.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            update.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            update.setTableName(this.getNode().getTableName());
        }
        update.setIndexName(this.getNode().getIndexUsed().getName());
        return update;
    }

    @Override
    public UpdateNode deepCopy() {
        UpdateNode delete = new UpdateNode(null);
        super.deepCopySelfTo(delete);
        return delete;
    }

    @Override
    public UpdateNode copy() {
        UpdateNode delete = new UpdateNode(null);
        super.copySelfTo(delete);
        return delete;
    }

}
