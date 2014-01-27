package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.misc.QueryExpression;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.fragment.Limit;
import com.alibaba.cobar.parser.ast.fragment.OrderBy;
import com.alibaba.cobar.parser.ast.fragment.tableref.IndexHint;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;

/**
 * 将cobar parser的语法树直接生成sql，允许替换表名
 * 
 * @author jianghang 2014-1-13 下午3:27:56
 * @since 5.0.0
 */
public class MysqlOutputVisitor extends MySQLOutputASTVisitor {

    // 表名替换使用，注意表名都需为大写
    private Map<String/* logic table */, String/* real table */> logicTable2RealTable;
    // 路由结果是否为单库单表,是则limit参数不做改变
    private boolean                                              singleNode;

    public MysqlOutputVisitor(StringBuilder appendable, boolean singleNode, Map<String, String> logicTable2RealTable){
        this(appendable, null, singleNode, logicTable2RealTable);
    }

    public MysqlOutputVisitor(StringBuilder appendable, Object[] args, boolean singleNode,
                              Map<String, String> logicTable2RealTable){
        super(appendable, args);
        this.singleNode = singleNode;
        this.logicTable2RealTable = logicTable2RealTable;
    }

    private String convertToRealTable(String tableName) {
        if (tableName == null) {
            return null;
        }

        if (logicTable2RealTable == null || logicTable2RealTable.isEmpty()) {
            return tableName;
        }

        // String upperTableName = tableName.toUpperCase();
        return logicTable2RealTable.get(tableName);
    }

    public void visit(TableRefFactor node) {
        // 逻辑表名
        Identifier table = node.getTable();
        // 表名替换
        String tableName = convertToRealTable(table.getIdText());
        if (tableName != null) {
            Identifier realTable = new Identifier(table.getParent(), tableName);
            realTable.accept(this);
        } else {
            table.accept(this);
        }
        String alias = node.getAlias();
        if (alias != null) {
            appendable.append(" AS ").append(alias);
        }
        List<IndexHint> list = node.getHintList();
        if (list != null && !list.isEmpty()) {
            appendable.append(' ');
            printList(list, " ");
        }
    }

    public void visit(Limit node) {
        appendable.append("LIMIT ");
        Object offset = node.getOffset();
        Object size = node.getSize();
        if (offset instanceof ParamMarker || size instanceof ParamMarker) {
            throw new OptimizerException("暂时不支持limit的占位符替换");
        }

        long offsetLong = ((Number) offset).longValue();
        long sizeLong = ((Number) size).longValue();
        if (!this.singleNode) {
            // 处理分库分表条件下的limit,对于路由结果是单库单表的情况，limit不做处理
            sizeLong += offsetLong;
            offsetLong = 0L;
        }
        appendable.append(offsetLong);
        appendable.append(", ");
        appendable.append(sizeLong);
    }

    @Override
    public void visit(DMLDeleteStatement node) {
        appendable.append("DELETE ");
        if (node.isLowPriority()) appendable.append("LOW_PRIORITY ");
        if (node.isQuick()) appendable.append("QUICK ");
        if (node.isIgnore()) appendable.append("IGNORE ");
        TableReferences tableRefs = node.getTableRefs();
        if (tableRefs == null) {
            appendable.append("FROM ");
            // 表名替换
            Identifier logicTable = node.getTableNames().get(0);
            String tableName = convertToRealTable(logicTable.getIdText());
            if (tableName != null) {
                Identifier realTable = new Identifier(logicTable.getParent(), tableName);
                realTable.accept(this);
            } else {
                logicTable.accept(this);
            }
        } else {
            printList(node.getTableNames());
            appendable.append(" FROM ");
            node.getTableRefs().accept(this);
        }
        Expression where = node.getWhereCondition();
        if (where != null) {
            appendable.append(" WHERE ");
            where.accept(this);
        }
        OrderBy orderBy = node.getOrderBy();
        if (orderBy != null) {
            appendable.append(' ');
            orderBy.accept(this);
        }
        Limit limit = node.getLimit();
        if (limit != null) {
            appendable.append(' ');
            limit.accept(this);
        }
    }

    @Override
    public void visit(DMLInsertStatement node) {
        appendable.append("INSERT ");
        switch (node.getMode()) {
            case DELAY:
                appendable.append("DELAYED ");
                break;
            case HIGH:
                appendable.append("HIGH_PRIORITY ");
                break;
            case LOW:
                appendable.append("LOW_PRIORITY ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
        if (node.isIgnore()) appendable.append("IGNORE ");
        appendable.append("INTO ");
        // 表名替换
        Identifier logicTable = node.getTable();
        String tableName = convertToRealTable(logicTable.getIdText());
        if (tableName != null) {
            Identifier realTable = new Identifier(logicTable.getParent(), tableName);
            realTable.accept(this);
        } else {
            logicTable.accept(this);
        }

        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty()) continue;
                    if (isFst) isFst = false;
                    else appendable.append(", ");
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for INSERT");
            }
        } else {
            select.accept(this);
        }

        List<Pair<Identifier, Expression>> dup = node.getDuplicateUpdate();
        if (dup != null && !dup.isEmpty()) {
            appendable.append(" ON DUPLICATE KEY UPDATE ");
            boolean isFst = true;
            for (Pair<Identifier, Expression> p : dup) {
                if (isFst) isFst = false;
                else appendable.append(", ");
                p.getKey().accept(this);
                appendable.append(" = ");
                p.getValue().accept(this);
            }
        }
    }

    @Override
    public void visit(DMLReplaceStatement node) {
        appendable.append("REPLACE ");
        switch (node.getMode()) {
            case DELAY:
                appendable.append("DELAYED ");
                break;
            case LOW:
                appendable.append("LOW_PRIORITY ");
                break;
            case UNDEF:
                break;
            default:
                throw new IllegalArgumentException("unknown mode for INSERT: " + node.getMode());
        }
        appendable.append("INTO ");
        // 表名替换
        Identifier logicTable = node.getTable();
        String tableName = convertToRealTable(logicTable.getIdText());
        if (tableName != null) {
            Identifier realTable = new Identifier(logicTable.getParent(), tableName);
            realTable.accept(this);
        } else {
            logicTable.accept(this);
        }
        appendable.append(' ');

        List<Identifier> cols = node.getColumnNameList();
        if (cols != null && !cols.isEmpty()) {
            appendable.append('(');
            printList(cols);
            appendable.append(") ");
        }

        QueryExpression select = node.getSelect();
        if (select == null) {
            appendable.append("VALUES ");
            List<RowExpression> rows = node.getRowList();
            if (rows != null && !rows.isEmpty()) {
                boolean isFst = true;
                for (RowExpression row : rows) {
                    if (row == null || row.getRowExprList().isEmpty()) continue;
                    if (isFst) isFst = false;
                    else appendable.append(", ");
                    appendable.append('(');
                    printList(row.getRowExprList());
                    appendable.append(')');
                }
            } else {
                throw new IllegalArgumentException("at least one row for REPLACE");
            }
        } else {
            select.accept(this);
        }
    }

    public void setLogicTable2RealTable(Map<String, String> logicTable2RealTable) {
        this.logicTable2RealTable = logicTable2RealTable;
    }

    public void setSingleNode(boolean singleNode) {
        this.singleNode = singleNode;
    }
}
