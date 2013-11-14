package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;

public class PutNode extends DMLNode<PutNode> {

    public PutNode(QueryTreeNode qtn){
        super(qtn);
    }

    public TableNode getQueryTreeNode() {
        return (TableNode) this.qtn;
    }

    public TableMeta getTableMeta() {
        return this.getQueryTreeNode().getTableMeta();
    }

    public IDataNodeExecutor toDataNodeExecutor() {
        IReplace put = ASTNodeFactory.getInstance().createReplace();
        // put.setDbName(this.getNode().getDbName());
        // put.setIndexName(this.getNode().getIndexUsed().getName());
        // put.setConsistent(true);
        // put.setUpdateColumns(this.getColumns());
        // put.setUpdateValues(this.getValues());
        // put.executeOn(this.getDataNode());
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
