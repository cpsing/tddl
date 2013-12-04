package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;

public class PutNode extends DMLNode<PutNode> {

    public PutNode(TableNode table){
        super(table);
    }

    public IDataNodeExecutor toDataNodeExecutor() {
        IReplace put = ASTNodeFactory.getInstance().createReplace();
        if (this.getNode().getActualTableName() != null) {
            put.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            put.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            put.setTableName(this.getNode().getTableName());
        }
        put.setIndexName(this.getNode().getIndexUsed().getName());
        put.setConsistent(true);
        put.setUpdateColumns(this.getColumns());
        put.setUpdateValues(this.getValues());
        put.executeOn(this.getDataNode());
        return put;
    }

    public PutNode deepCopy() {
        PutNode put = new PutNode(null);
        super.deepCopySelfTo(put);
        return put;
    }

    public PutNode copy() {
        PutNode put = new PutNode(null);
        super.copySelfTo(put);
        return put;
    }

}
