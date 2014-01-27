package com.taobao.tddl.optimizer.parse.cobar.visitor;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

/**
 * delete处理
 * 
 * @since 5.0.0
 */
public class MySqlDeleteVisitor extends EmptySQLASTVisitor {

    private DeleteNode deleteNode;

    public void visit(DMLDeleteStatement node) {
        TableNode table = null;
        if (node.getTableNames().size() == 1) {
            table = getTableNode(node.getTableNames().get(0).getIdTextUpUnescape());
        } else {
            throw new NotSupportException("not support multi table delete");
        }

        Expression expr = node.getWhereCondition();
        if (expr != null) {
            handleCondition(expr, table);
        }

        this.deleteNode = table.delete();
    }

    private TableNode getTableNode(String tableName) {
        return new TableNode(tableName);
    }

    private void handleCondition(Expression expr, TableNode table) {
        MySqlExprVisitor mv = new MySqlExprVisitor();
        expr.accept(mv);
        table.query(mv.getFilter());
    }

    public DeleteNode getDeleteNode() {
        return deleteNode;
    }
}
