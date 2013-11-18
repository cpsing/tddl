package com.taobao.tddl.optimizer.core.ast.dml;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class DeleteNode extends DMLNode<DeleteNode> {

    public DeleteNode(QueryTreeNode qtn){
        super(qtn);
    }

    public TableNode getNode() {
        return (TableNode) this.qtn;
    }

    public TableMeta getTableMeta() {
        return this.getNode().getTableMeta();
    }

    public IDataNodeExecutor toDataNodeExecutor() throws QueryException {
        IDelete delete = ASTNodeFactory.getInstance().createDelete();
        delete.setConsistent(true);
        delete.executeOn(this.getDataNode());
        delete.setQueryTree((IQueryTree) this.getNode().toDataNodeExecutor());
        // FIX by shenxun :没有使用真实表名
        delete.setSchemaName(((TableNode) this.getNode()).getSchemaName());
        delete.setIndexName(((TableNode) this.getNode()).getIndexUsed().getName());

        return delete;
    }

    public DeleteNode setQuery(TableNode query) {
        this.qtn = query;
        return this;
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
