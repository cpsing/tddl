package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.List;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class UpdateNode extends DMLNode<UpdateNode> {

    public UpdateNode(QueryTreeNode qtn){
        super(qtn);
    }

    public TableNode getNode() {
        return (TableNode) this.qtn;
    }

    public TableMeta getTableMeta() {
        return this.getNode().getTableMeta();
    }

    public UpdateNode setUpdateColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getUpdateColumns() {
        return this.columns;
    }

    public UpdateNode setUpdateValues(List<Comparable> values) {
        this.values = values;
        return this;
    }

    public List<Comparable> getUpdateValues() {
        return this.values;
    }

    public IDataNodeExecutor toDataNodeExecutor() throws QueryException {
        convertTypeToSatifyColumnMeta(this.getUpdateColumns(), values);
        IUpdate update = ASTNodeFactory.getInstance().createUpdate();
        for (ISelectable updateColumn : this.getColumns()) {

            if (((TableNode) this.getNode()).getTableMeta().getPrimaryIndex().getPartitionColumns() != null) {
                if (((TableNode) this.getNode()).getTableMeta()
                    .getPrimaryIndex()
                    .getPartitionColumns()
                    .contains(updateColumn.getColumnName()))

                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是分库键，不允许修改");
            }
            if (((TableNode) this.getNode()).getTableMeta()
                .getPrimaryKeyMap()
                .containsKey(updateColumn.getColumnName())) {
                throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是主键，不允许修改");
            }
        }

        update.setConsistent(true);
        update.executeOn(this.getNode().getDataNode());
        update.setQueryTree(this.getNode().toDataNodeExecutor());
        update.setUpdateColumns(this.getUpdateColumns());
        update.setUpdateValues(values);
        update.setSchemaName(((TableNode) this.getNode()).getSchemaName());
        update.setIndexName(((KVIndexNode) this.getNode()).getIndexUsed().getName());
        return update;
    }

    public UpdateNode deepCopy() {
        UpdateNode delete = new UpdateNode(null);
        super.deepCopySelfTo(delete);
        return delete;
    }

    public UpdateNode copy() {
        UpdateNode delete = new UpdateNode(null);
        super.copySelfTo(delete);
        return delete;
    }

}
