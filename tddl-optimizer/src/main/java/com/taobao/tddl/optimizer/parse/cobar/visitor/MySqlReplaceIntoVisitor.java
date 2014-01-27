package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

/**
 * replace处理
 * 
 * @since 5.0.0
 */
public class MySqlReplaceIntoVisitor extends EmptySQLASTVisitor {

    private PutNode replaceNode;

    public void visit(DMLReplaceStatement node) {
        TableNode table = getTableNode(node);
        String insertColumns = this.getInsertColumnsStr(node);
        List<RowExpression> exprList = node.getRowList();
        if (exprList != null && exprList.size() == 1) {
            RowExpression expr = exprList.get(0);
            Comparable[] iv = getRowValue(expr);
            this.replaceNode = table.put(insertColumns, iv);
        } else {
            throw new NotSupportException("could not support multi row values.");
        }
    }

    private TableNode getTableNode(DMLReplaceStatement node) {
        return new TableNode(node.getTable().getIdTextUpUnescape());
    }

    private String getInsertColumnsStr(DMLReplaceStatement node) {
        List<Identifier> columnNames = node.getColumnNameList();
        StringBuilder sb = new StringBuilder("");
        if (columnNames != null && columnNames.size() != 0) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) {
                    sb.append(" ");
                }
                sb.append(columnNames.get(i).getIdTextUpUnescape());
            }
        }

        return sb.toString();
    }

    private Comparable[] getRowValue(RowExpression expr) {
        Comparable[] iv = new Comparable[expr.getRowExprList().size()];
        for (int i = 0; i < expr.getRowExprList().size(); i++) {
            MySqlExprVisitor mv = new MySqlExprVisitor();
            expr.getRowExprList().get(i).accept(mv);
            Object obj = mv.getColumnOrValue();
            iv[i] = (Comparable) obj;
        }
        return iv;
    }

    public PutNode getReplaceNode() {
        return replaceNode;
    }
}
