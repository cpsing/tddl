package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;

public class DeleteNode extends DMLNode<DeleteNode> {

    public DeleteNode(QueryTreeNode qtn){
        super(qtn);
    }

    public TableNode getQueryTreeNode() {
        return (TableNode) this.qtn;
    }

    public TableMeta getTableMeta() {
        return this.getQueryTreeNode().getTableMeta();
    }

    public IDataNodeExecutor toDataNodeExecutor() {
        IDelete delete = ASTNodeFactory.getInstance().createDelete();
        // delete.setConsistent(true);
        // delete.executeOn(this.getDataNode());
        // delete.setQueryCommon(this.getNode().toDataNodeExecutor());
        // // FIX by shenxun :没有使用真实表名
        //
        // delete.setDbName(((TableNode) this.getNode()).getDbName());
        // delete.setIndexName(((TableNode)
        // this.getNode()).getIndexUsed().getName());
        //
        return delete;
    }

    public DeleteNode deepCopy() {
        DeleteNode delete = new DeleteNode(null);
        super.deepCopySelfTo(delete);
        return delete;
    }

    public DeleteNode copy() {
        DeleteNode delete = new DeleteNode(null);
        super.copySelfTo(delete);
        return delete;
    }

}
