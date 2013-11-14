package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.List;

import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public class UpdateNode extends DMLNode<UpdateNode> {

    public UpdateNode(QueryTreeNode qtn){
        super(qtn);
    }

    public TableNode getQueryTreeNode() {
        return (TableNode) this.qtn;
    }

    public TableMeta getTableMeta() {
        return this.getQueryTreeNode().getTableMeta();
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
        // TODO
        IUpdate update = ASTNodeFactory.getInstance().createUpdate();
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
