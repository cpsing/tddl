package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.fragment.GroupBy;
import com.alibaba.cobar.parser.ast.fragment.Limit;
import com.alibaba.cobar.parser.ast.fragment.OrderBy;
import com.alibaba.cobar.parser.ast.fragment.SortOrder;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement.SelectDuplicationStrategy;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement.SelectOption;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectUnionStatement;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * select表达式的解析
 * 
 * @since 5.0.0
 */
public class MySqlSelectVisitor extends EmptySQLASTVisitor {

    private QueryTreeNode tableNode;

    public QueryTreeNode getTableNode() {
        return tableNode;
    }

    @Override
    public void visit(DMLSelectStatement node) {
        TableReferences tables = node.getTables();
        if (tables != null) {
            handleFrom(tables);
        }

        List<Pair<Expression, String>> items = node.getSelectExprList();
        if (items != null) {
            List<ISelectable> selectItems = handleSelectItems(items);
            handleSelectOption(node.getOption(), selectItems);

            if (selectItems != null) {
                tableNode.select(selectItems);
            }
        }

        Expression whereExpr = node.getWhere();
        if (whereExpr != null) {
            handleWhereCondition(whereExpr);
        }

        OrderBy orderBy = node.getOrder();
        if (orderBy != null) {
            handleOrderBy(orderBy);
        }

        GroupBy groupBy = node.getGroup();
        if (groupBy != null) {
            handleGroupBy(groupBy);
        }

        Expression havingExpr = node.getHaving();
        if (havingExpr != null) {
            handleHavingCondition(havingExpr);
        }

        Limit limit = node.getLimit();
        if (limit != null) {
            handleLimit(limit);
        }
    }

    @Override
    public void visit(DMLSelectUnionStatement node) {
        throw new NotSupportException();
    }

    // ===================== helper =======================

    private void handleFrom(TableReferences tables) {
        List<TableReference> trs = tables.getTableReferenceList();
        for (int i = 0; i < trs.size(); i++) {
            TableReference tr = trs.get(i);
            MySqlExprVisitor mtv = new MySqlExprVisitor();
            tr.accept(mtv);
            if (this.tableNode == null) {
                this.tableNode = mtv.getTableNode();
                // 如果是第一个table，并且是唯一的一个，才做queryNode，因为如果多于两个可以通过joinNode来代替
                if (this.tableNode.isSubQuery() && i == trs.size() - 1) {
                    this.tableNode = new QueryNode(this.tableNode);
                }
            } else {
                this.tableNode = this.tableNode.join(mtv.getTableNode());
            }
        }
    }

    private List<ISelectable> handleSelectItems(List<Pair<Expression, String>> items) {
        List<ISelectable> selectItems = new ArrayList<ISelectable>();
        for (Pair<Expression, String> item : items) {
            Expression expr = item.getKey();

            MySqlExprVisitor ev = new MySqlExprVisitor();
            expr.accept(ev);
            Comparable obj = ev.getColumnOrValue();
            if (!(obj instanceof ISelectable)) { // 常量先转成booleanFilter
                obj = ev.buildConstanctFilter(obj);
            }

            ((ISelectable) obj).setAlias(item.getValue());
            selectItems.add((ISelectable) obj);
        }

        return selectItems;
    }

    private void handleSelectOption(SelectOption option, List<ISelectable> selectItems) {
        if (option.resultDup == SelectDuplicationStrategy.DISTINCT) {
            for (ISelectable obj : selectItems) {
                obj.setDistinct(true);
            }
        }
    }

    private void handleWhereCondition(Expression whereExpr) {
        MySqlExprVisitor mev = new MySqlExprVisitor();
        whereExpr.accept(mev);
        if (this.tableNode != null) {
            IFilter whereFilter = null;
            if (mev.getFilter() != null) {
                whereFilter = mev.getFilter();
            } else if (mev.getColumnOrValue() != null) {
                whereFilter = mev.buildConstanctFilter(mev.getColumnOrValue());
            }

            tableNode.query(whereFilter);
            this.tableNode.setAllWhereFilter(tableNode.getWhereFilter());
        } else {
            throw new IllegalArgumentException("from expression is null,check the sql!");
        }
    }

    private void handleOrderBy(OrderBy orderBy) {
        List<Pair<Expression, SortOrder>> olist = orderBy.getOrderByList();
        for (Pair<Expression, SortOrder> p : olist) {
            Expression expr = p.getKey();
            MySqlExprVisitor v = new MySqlExprVisitor();
            expr.accept(v);
            SortOrder sorder = p.getValue();
            this.tableNode = tableNode.orderBy((ISelectable) v.getColumnOrValue(),
                sorder == SortOrder.ASC ? true : false);
        }
    }

    private void handleGroupBy(GroupBy groupBy) {
        List<Pair<Expression, SortOrder>> glist = groupBy.getOrderByList();
        for (Pair<Expression, SortOrder> p : glist) {
            Expression expr = p.getKey();
            MySqlExprVisitor v = new MySqlExprVisitor();
            expr.accept(v);
            SortOrder sorder = p.getValue();
            this.tableNode = tableNode.groupBy((ISelectable) v.getColumnOrValue(),
                sorder == SortOrder.ASC ? true : false);
        }

        if (groupBy.isWithRollup()) {
            throw new NotSupportException("with rollup is not supported yet!");
        }
    }

    private void handleHavingCondition(Expression havingExpr) {
        MySqlExprVisitor mev = new MySqlExprVisitor();
        havingExpr.accept(mev);
        IFilter havingFilter = mev.getFilter();
        if (this.tableNode == null) {
            throw new IllegalArgumentException("from expression is null,check the sql!");
        }

        if (havingFilter != null) {
            this.tableNode = this.tableNode.having(havingFilter);
        } else if (mev.getColumnOrValue() != null) {
            this.tableNode = this.tableNode.having(mev.buildConstanctFilter(mev.getColumnOrValue()));
        }

    }

    private void handleLimit(Limit limit) {
        if (limit.getOffset() instanceof ParamMarker) {
            tableNode.setLimitFrom(ASTNodeFactory.getInstance()
                .createBindValue(((ParamMarker) limit.getOffset()).getParamIndex()));
        } else {
            tableNode.setLimitFrom(Long.valueOf(String.valueOf(limit.getOffset())));
        }

        if (limit.getSize() instanceof ParamMarker) {
            tableNode.setLimitTo(ASTNodeFactory.getInstance()
                .createBindValue(((ParamMarker) limit.getSize()).getParamIndex()));
        } else {
            tableNode.setLimitTo(Long.valueOf(String.valueOf(limit.getSize())));
        }
    }

}
