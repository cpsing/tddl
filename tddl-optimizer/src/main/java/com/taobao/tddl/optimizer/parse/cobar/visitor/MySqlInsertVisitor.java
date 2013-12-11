package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.misc.QueryExpression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

public class MySqlInsertVisitor extends EmptySQLASTVisitor {

    private InsertNode insertNode;

    public void visit(DMLInsertStatement node) {
        TableNode table = getTableNode(node);
        String insertColumns = this.getInsertColumnsStr(node);
        List<RowExpression> exprList = node.getRowList();
        if (exprList != null && exprList.size() == 1) {
            RowExpression expr = exprList.get(0);
            Comparable[] iv = getRowValue(expr);
            this.insertNode = table.insert(insertColumns, iv);
        } else {
            throw new NotSupportException("could not support multi row values.");
        }

        // 暂时不支持子表的查询
        QueryExpression subQuery = node.getSelect();
        if (subQuery != null) {
            throw new NotSupportException("could not support insert into select");
        }
    }

    private TableNode getTableNode(DMLInsertStatement node) {
        TableNode table = null;
        table = new TableNode(node.getTable().getIdTextUpUnescape());
        return table;
    }

    private String getInsertColumnsStr(DMLInsertStatement node) {
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

    public InsertNode getInsertNode() {
        return insertNode;
    }
}
