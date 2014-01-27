package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

/**
 * update类型处理
 * 
 * @since 5.0.0
 */
public class MySqlUpdateVisitor extends EmptySQLASTVisitor {

    private UpdateNode updateNode;

    public void visit(DMLUpdateStatement node) {
        QueryTreeNode table = getTableNode(node);
        List<Pair<Identifier, Expression>> cvs = node.getValues();
        Comparable[] updateValues = new Comparable[cvs.size()];
        StringBuilder updateColumnsSb = new StringBuilder();
        for (int i = 0; i < cvs.size(); i++) {
            Pair<Identifier, Expression> p = cvs.get(i);
            if (i > 0) {
                updateColumnsSb.append(" ");
            }
            updateColumnsSb.append(p.getKey().getIdTextUpUnescape());
            MySqlExprVisitor mv = new MySqlExprVisitor();
            p.getValue().accept(mv);
            updateValues[i] = (Comparable) mv.getColumnOrValue();// 可能为function
        }

        Expression expr = node.getWhere();
        if (expr != null) {
            handleCondition(table, expr);
        }

        this.updateNode = ((TableNode) table).update(updateColumnsSb.toString(), updateValues);
    }

    private QueryTreeNode getTableNode(DMLUpdateStatement node) {
        TableReferences trs = node.getTableRefs();
        List<TableReference> tbls = trs.getTableReferenceList();
        QueryTreeNode table = null;
        if (tbls != null && tbls.size() == 1) {
            MySqlExprVisitor tv = new MySqlExprVisitor();
            tbls.get(0).accept(tv);
            table = tv.getTableNode();
        } else {
            throw new NotSupportException("not support more than one table update!");
        }

        return table;
    }

    private void handleCondition(QueryTreeNode table, Expression expr) {
        MySqlExprVisitor mv = new MySqlExprVisitor();
        expr.accept(mv);
        table.query(mv.getFilter());
    }

    public UpdateNode getUpdateNode() {
        return updateNode;
    }

}
