package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;

public class InsertNode extends DMLNode<InsertNode> {

    private boolean createPk = true; // 是否为自增长字段，暂时不支持

    public InsertNode(TableNode table){
        super(table);
    }

    public IDataNodeExecutor toDataNodeExecutor() {
        IInsert insert = ASTNodeFactory.getInstance().createInsert();
        if (this.getNode().getActualTableName() != null) {
            insert.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            insert.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            insert.setTableName(this.getNode().getTableName());
        }
        insert.setIndexName((this.getNode()).getIndexUsed().getName());
        insert.setConsistent(true);
        insert.setUpdateColumns(this.getColumns());
        insert.setUpdateValues(this.getValues());
        insert.executeOn(this.getDataNode());
        return insert;
    }

    public InsertNode deepCopy() {
        InsertNode insert = new InsertNode(null);
        super.deepCopySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());
        return insert;
    }

    public InsertNode copy() {
        InsertNode insert = new InsertNode(null);
        super.copySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());
        return insert;
    }

    public boolean isCreatePk() {
        return createPk;
    }

    public InsertNode setCreatePk(boolean createPk) {
        this.createPk = createPk;
        return this;
    }

}
