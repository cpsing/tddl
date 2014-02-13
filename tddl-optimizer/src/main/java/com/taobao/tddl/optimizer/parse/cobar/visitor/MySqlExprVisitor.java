package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.MethodUtils;

import com.alibaba.cobar.parser.ast.expression.BinaryOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.PolyadicOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.UnaryOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticAddExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticDivideExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticIntegerDivideExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticModExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticMultiplyExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.ArithmeticSubtractExpression;
import com.alibaba.cobar.parser.ast.expression.arithmeic.MinusExpression;
import com.alibaba.cobar.parser.ast.expression.bit.BitAndExpression;
import com.alibaba.cobar.parser.ast.expression.bit.BitInvertExpression;
import com.alibaba.cobar.parser.ast.expression.bit.BitOrExpression;
import com.alibaba.cobar.parser.ast.expression.bit.BitShiftExpression;
import com.alibaba.cobar.parser.ast.expression.bit.BitXORExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.BetweenAndExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionGreaterThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionGreaterThanOrEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionIsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessOrGreaterThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessThanExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionLessThanOrEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionNotEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.ComparisionNullSafeEqualsExpression;
import com.alibaba.cobar.parser.ast.expression.comparison.InExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalAndExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalNotExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalOrExpression;
import com.alibaba.cobar.parser.ast.expression.logical.LogicalXORExpression;
import com.alibaba.cobar.parser.ast.expression.logical.NegativeValueExpression;
import com.alibaba.cobar.parser.ast.expression.misc.AssignmentExpression;
import com.alibaba.cobar.parser.ast.expression.misc.InExpressionList;
import com.alibaba.cobar.parser.ast.expression.misc.QueryExpression;
import com.alibaba.cobar.parser.ast.expression.misc.SubqueryAllExpression;
import com.alibaba.cobar.parser.ast.expression.misc.SubqueryAnyExpression;
import com.alibaba.cobar.parser.ast.expression.primary.CaseWhenOperatorExpression;
import com.alibaba.cobar.parser.ast.expression.primary.ExistsPrimary;
import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.expression.primary.MatchExpression;
import com.alibaba.cobar.parser.ast.expression.primary.ParamMarker;
import com.alibaba.cobar.parser.ast.expression.primary.RowExpression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Avg;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Count;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Max;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Min;
import com.alibaba.cobar.parser.ast.expression.primary.function.groupby.Sum;
import com.alibaba.cobar.parser.ast.expression.primary.literal.IntervalPrimary;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralBitField;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralBoolean;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralHexadecimal;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNull;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralNumber;
import com.alibaba.cobar.parser.ast.expression.primary.literal.LiteralString;
import com.alibaba.cobar.parser.ast.expression.string.LikeExpression;
import com.alibaba.cobar.parser.ast.expression.type.CastBinaryExpression;
import com.alibaba.cobar.parser.ast.fragment.tableref.Dual;
import com.alibaba.cobar.parser.ast.fragment.tableref.IndexHint;
import com.alibaba.cobar.parser.ast.fragment.tableref.InnerJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.NaturalJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.OuterJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.StraightJoin;
import com.alibaba.cobar.parser.ast.fragment.tableref.SubqueryFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.recognizer.mysql.lexer.MySQLLexer;
import com.alibaba.cobar.parser.recognizer.mysql.syntax.MySQLExprParser;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;
import com.google.common.collect.Maps;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.LobVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;

/**
 * 解析mysql表达式
 * 
 * <pre>
 * 参考资料：
 * 1. https://dev.mysql.com/doc/refman/5.6/en/comparison-operators.html
 * </pre>
 * 
 * @since 5.0.0
 */
public class MySqlExprVisitor extends EmptySQLASTVisitor {

    private static Map    emptyMap  = Maps.newHashMap();
    private Comparable    columnOrValue;
    private QueryTreeNode tableNode = null;
    private String        valueForLike;
    private IFilter       filter;

    @Override
    public void visit(BetweenAndExpression node) {
        Expression first = node.getFirst();
        Expression second = node.getSecond();
        Expression third = node.getThird();

        MySqlExprVisitor v = new MySqlExprVisitor();
        first.accept(v);
        Comparable col = v.getColumnOrValue();

        MySqlExprVisitor lv = new MySqlExprVisitor();
        second.accept(lv);
        Comparable lval = lv.getColumnOrValue();

        MySqlExprVisitor rv = new MySqlExprVisitor();
        third.accept(rv);
        Comparable rval = rv.getColumnOrValue();

        IBooleanFilter left = this.buildBooleanFilter(col, lval, OPERATION.GT_EQ, node);
        IBooleanFilter right = this.buildBooleanFilter(col, rval, OPERATION.LT_EQ, node);
        ILogicalFilter ilf = buildLogicalFilter(left, right, OPERATION.AND, node);
        if (node.isNot()) {
            ilf.setIsNot(true);
        }
        this.filter = ilf;
    }

    @Override
    public void visit(ComparisionIsExpression node) {
        IBooleanFilter ibf = ASTNodeFactory.getInstance().createBooleanFilter();
        MySqlExprVisitor leftEvi = new MySqlExprVisitor();
        node.getOperand().accept(leftEvi);
        ibf.setColumn(leftEvi.getColumnOrValue());
        if (node.getMode() == ComparisionIsExpression.IS_NULL) {
            ibf.setOperation(OPERATION.IS_NULL);
            // ibf.setValue(NullValue.getNullValue());
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_NOT_NULL) {
            ibf.setOperation(OPERATION.IS_NOT_NULL);
            // ibf.setValue(NullValue.getNullValue());
            // ibf.setIsNot(true);
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_FALSE) {
            ibf.setOperation(OPERATION.IS_FALSE);
            // ibf.setValue(false);
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_NOT_FALSE) {
            ibf.setOperation(OPERATION.IS_NOT_FALSE);
            // ibf.setValue(new Boolean(false));
            // ibf.setIsNot(true);
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_TRUE) {
            ibf.setOperation(OPERATION.IS_TRUE);
            // ibf.setValue(new Boolean(true));
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_NOT_TRUE) {
            ibf.setOperation(OPERATION.IS_NOT_TRUE);
            // ibf.setValue(new Boolean(true));
            // ibf.setIsNot(true);
            ibf.setIsNot(false);
        } else if (node.getMode() == ComparisionIsExpression.IS_UNKNOWN) {
            throw new NotSupportException("not support IS_UNKNOWN");
        } else if (node.getMode() == ComparisionIsExpression.IS_NOT_UNKNOWN) {
            throw new NotSupportException("not support IS_NOT_UNKNOWN");
        }

        this.filter = ibf;
    }

    @Override
    public void visit(BinaryOperatorExpression node) {
        if (eval(node)) { // 计算出了结果
            return;
        }

        if (node instanceof ComparisionEqualsExpression) {
            this.handleBooleanFilter(node, OPERATION.EQ);
        } else if (node instanceof ComparisionGreaterThanExpression) {
            this.handleBooleanFilter(node, OPERATION.GT);
        } else if (node instanceof ComparisionGreaterThanOrEqualsExpression) {
            this.handleBooleanFilter(node, OPERATION.GT_EQ);
        } else if (node instanceof ComparisionLessOrGreaterThanExpression) {
            this.handleBooleanFilter(node, OPERATION.NOT_EQ);
        } else if (node instanceof ComparisionLessThanExpression) {
            this.handleBooleanFilter(node, OPERATION.LT);
        } else if (node instanceof ComparisionLessThanOrEqualsExpression) {
            this.handleBooleanFilter(node, OPERATION.LT_EQ);
        } else if (node instanceof ComparisionNotEqualsExpression) {
            this.handleBooleanFilter(node, OPERATION.NOT_EQ);
        } else if (node instanceof ComparisionNullSafeEqualsExpression) {
            throw new NotSupportException("not support '<=>' ");
        } else if (node instanceof ArithmeticAddExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.ADD);
        } else if (node instanceof ArithmeticDivideExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.DIVISION);
        } else if (node instanceof ArithmeticIntegerDivideExpression) {
            this.handleArithmetric(node, node.getOperator());
        } else if (node instanceof ArithmeticModExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.MOD);
        } else if (node instanceof ArithmeticMultiplyExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.MULTIPLY);
        } else if (node instanceof ArithmeticSubtractExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.SUB);
        } else if (node instanceof AssignmentExpression) {
            throw new NotSupportException("not support ':=' ");
        } else if (node instanceof BitAndExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.BITAND);
        } else if (node instanceof BitOrExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.BITOR);
        } else if (node instanceof BitShiftExpression) {
            if (((BitShiftExpression) node).isRightShift()) {
                this.handleArithmetric(node, IFunction.BuiltInFunction.BITRSHIFT);
            } else {
                this.handleArithmetric(node, IFunction.BuiltInFunction.BITLSHIFT);
            }
        } else if (node instanceof BitXORExpression) {
            this.handleArithmetric(node, IFunction.BuiltInFunction.BITXOR);
        } else if (node instanceof InExpression) {
            this.handleInExpression((InExpression) node);
        } else if (node instanceof LogicalXORExpression) {
            this.handleBooleanFilter(node, OPERATION.XOR);
        } else {
            throw new NotSupportException("not supported this BinaryOperatorExpression type " + node.getOperator());
        }
    }

    @Override
    public void visit(UnaryOperatorExpression node) {
        if (eval(node)) { // 计算出了结果
            return;
        }

        if (node instanceof BitInvertExpression) {
        } else if (node instanceof CastBinaryExpression) {
        } else if (node instanceof LogicalNotExpression) {
        } else if (node instanceof MinusExpression) {
        } else if (node instanceof NegativeValueExpression) {
            throw new NotSupportException("not support NegativeValueExpression");
        } else if (node instanceof SubqueryAllExpression) {
            throw new NotSupportException("not support SubqueryAllExpression");
        } else if (node instanceof SubqueryAnyExpression) {
            throw new NotSupportException("not support SubqueryAnyExpression");
        } else {
            throw new NotSupportException("not supported this UnaryOperatorExpression type " + node.getOperator());
        }

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        if (node instanceof MinusExpression) {
            f.setFunctionName(IFunction.BuiltInFunction.MINUS);
        } else {
            f.setFunctionName(node.getOperator());
        }
        MySqlExprVisitor ev = new MySqlExprVisitor();
        node.getOperand().accept(ev);
        Object arg = ev.getColumnOrValue();
        List args = new ArrayList(1);
        args.add(arg);
        f.setArgs(args);
        f.setColumnName(this.getSqlExprStr(node));
        columnOrValue = f;
    }

    @Override
    public void visit(FunctionExpression node) {
        boolean argDistinct = isDistinct(node);
        IFunction ifunc = ASTNodeFactory.getInstance().createFunction();
        ifunc.setFunctionName(node.getFunctionName());
        List<Expression> expressions = node.getArguments();

        if (expressions != null) {
            ArrayList<Object> args = new ArrayList<Object>();
            for (Expression expr : expressions) {
                MySqlExprVisitor v = new MySqlExprVisitor();
                expr.accept(v);
                Object cv = v.getColumnOrValue();
                if ((cv instanceof ISelectable)) {
                    ((ISelectable) cv).setDistinct(argDistinct);
                }
                args.add(v.getColumnOrValue());
            }
            ifunc.setArgs(args);
        } else {
            ifunc.setArgs(new ArrayList());
        }

        ifunc.setColumnName(getSqlExprStr(node));
        columnOrValue = ifunc;
    }

    @Override
    public void visit(RowExpression node) {
        IFunction ifunc = ASTNodeFactory.getInstance().createFunction();
        ifunc.setFunctionName(IFunction.BuiltInFunction.ROW);
        List<Comparable> args = new ArrayList<Comparable>();
        for (int i = 0; i < node.getRowExprList().size(); i++) {
            MySqlExprVisitor mv = new MySqlExprVisitor();
            node.getRowExprList().get(i).accept(mv);
            Object obj = mv.getColumnOrValue();
            args.add((Comparable) obj);
        }
        ifunc.setArgs(args);
        ifunc.setColumnName(getSqlExprStr(node));
        columnOrValue = ifunc;
    }

    @Override
    public void visit(Identifier node) {
        IColumn column = ASTNodeFactory.getInstance().createColumn();
        if (node.getParent() != null) { // table.column
            column.setTableName(node.getParent().getIdTextUpUnescape());
        }
        column.setColumnName(node.getIdTextUpUnescape());
        this.columnOrValue = column;
    }

    @Override
    public void visit(InnerJoin node) { // inner join
        TableReference ltable = node.getLeftTableRef();
        TableReference rtable = node.getRightTableRef();
        JoinNode joinNode = commonJoin(ltable, rtable);

        Expression cond = node.getOnCond();
        if (cond != null) {
            MySqlExprVisitor ev = new MySqlExprVisitor();
            cond.accept(ev);
            IFilter ifilter = ev.getFilter();
            joinNode.setInnerJoin();
            addJoinOnColumns(ifilter, joinNode);
        } else if (node.getUsing() != null && node.getUsing().size() != 0) {
            IFilter ifilter = this.getUsingFilter(node.getUsing());
            joinNode.setInnerJoin();
            addJoinOnColumns(ifilter, joinNode);
        }
        this.tableNode = joinNode;
    }

    @Override
    public void visit(NaturalJoin node) { // 默认是同名字段完全匹配的 INNER JOIN
        TableReference ltable = node.getLeftTableRef();
        TableReference rtable = node.getRightTableRef();
        JoinNode joinNode = commonJoin(ltable, rtable);
        this.tableNode = joinNode;
    }

    @Override
    public void visit(OuterJoin node) {// left/right join
        TableReference ltable = node.getLeftTableRef();
        TableReference rtable = node.getRightTableRef();
        JoinNode joinNode = commonJoin(ltable, rtable);

        Expression cond = node.getOnCond();
        if (cond != null) {
            MySqlExprVisitor ev = new MySqlExprVisitor();
            cond.accept(ev);
            IFilter ifilter = ev.getFilter();
            if (node.isLeftJoin()) {
                joinNode.setLeftOuterJoin();
            } else {
                joinNode.setRightOuterJoin();
            }

            addJoinOnColumns(ifilter, joinNode);
        } else if (node.getUsing() != null && node.getUsing().size() != 0) {
            IFilter ifilter = this.getUsingFilter(node.getUsing());
            if (node.isLeftJoin()) {
                joinNode.setLeftOuterJoin();
            } else {
                joinNode.setRightOuterJoin();
            }

            addJoinOnColumns(ifilter, joinNode);
        }

        this.tableNode = joinNode;
    }

    @Override
    public void visit(StraightJoin node) {// inner join
        TableReference ltable = node.getLeftTableRef();
        TableReference rtable = node.getRightTableRef();
        JoinNode joinNode = commonJoin(ltable, rtable);
        Expression cond = node.getOnCond();
        if (cond != null) {
            MySqlExprVisitor ev = new MySqlExprVisitor();
            cond.accept(ev);
            IFilter ifilter = ev.getFilter();
            joinNode.setInnerJoin();
            addJoinOnColumns(ifilter, joinNode);
        }

        this.tableNode = joinNode;
    }

    @Override
    public void visit(IntervalPrimary node) {
        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setColumnName(this.getSqlExprStr(node));
        f.setFunctionName(IFunction.BuiltInFunction.INTERVAL);
        Expression quantity = node.getQuantity();
        MySqlExprVisitor ev = new MySqlExprVisitor();
        quantity.accept(ev);
        List args = new ArrayList(2);
        args.add(ev.getColumnOrValue());
        args.add(node.getUnit().toString());
        f.setArgs(args);
        this.columnOrValue = f;
    }

    @Override
    public void visit(LikeExpression node) {
        MySqlExprVisitor first = new MySqlExprVisitor();
        node.getFirst().accept(first);

        MySqlExprVisitor second = new MySqlExprVisitor();
        node.getSecond().accept(second);
        Comparable value = null;
        if (second.getValueForLike() != null) {
            value = second.getValueForLike();
        } else {
            value = second.getColumnOrValue();
        }

        this.filter = this.buildBooleanFilter(first.getColumnOrValue(), value, OPERATION.LIKE, node);
        if (node.isNot()) {
            this.filter.setIsNot(true);
        }

        // TODO: 支持下like下的ESCAPE参数
        // just like: higherPreExpr 'NOT'? 'LIKE' higherPreExpr ('ESCAPE'
        // higherPreExpr)?
    }

    @Override
    public void visit(LiteralBitField node) {
        if (node.getIntroducer() != null) {
            throw new NotSupportException("bit value not support introducer:" + node.getIntroducer());
        }
        this.columnOrValue = new LobVal(node.getText(), "b");
    }

    @Override
    public void visit(LiteralBoolean node) {
        this.columnOrValue = node.isTrue();
    }

    @Override
    public void visit(LiteralHexadecimal node) {
        if (node.getIntroducer() != null) {
            throw new NotSupportException("hex value not support introducer:" + node.getIntroducer());
        }

        this.columnOrValue = new LobVal(node.getText(), "x");
    }

    @Override
    public void visit(LiteralNull node) {
        this.columnOrValue = NullValue.getNullValue();
    }

    @Override
    public void visit(LiteralNumber node) {
        this.columnOrValue = (Comparable) node.getNumber();
    }

    @Override
    public void visit(LiteralString node) {
        if (node.getIntroducer() != null) {
            this.columnOrValue = new LobVal(node.getString(), node.getIntroducer());
        } else {
            // 由于是绑定变量的形式，普通的操作符要用转义后的，但是like的不行，似乎like比较特殊....
            this.columnOrValue = node.getUnescapedString();
            this.valueForLike = node.getString();
        }
    }

    @Override
    public void visit(ParamMarker node) {
        IBindVal val = ASTNodeFactory.getInstance().createBindValue(node.getParamIndex());
        columnOrValue = val;
    }

    @Override
    public void visit(PolyadicOperatorExpression node) {
        if (eval(node)) {// 已经计算出了结果
            return;
        }

        IFilter root = null;
        for (int i = 0; i < node.getArity(); i++) {
            Expression expr = node.getOperand(i);
            MySqlExprVisitor ev = new MySqlExprVisitor();
            expr.accept(ev);
            if (expr instanceof LiteralBoolean) {
                if ((Boolean) ev.getColumnOrValue()) {
                    ev.filter = this.buildConstanctFilter(1);
                } else {
                    ev.filter = this.buildConstanctFilter(0);
                }
            }

            if (ev.getFilter() == null) {
                if (ev.getColumnOrValue() != null) {
                    // a or b , a封到一个booleanFilter里
                    ev.filter = this.buildBooleanFilter(ev.getColumnOrValue(), null, OPERATION.CONSTANT, node);
                } else {
                    throw new NotSupportException("and/or不能没有操作符");
                }
            }
            if (root == null) {
                root = ev.getFilter();
            } else {
                if (node instanceof LogicalAndExpression) {
                    root = this.buildLogicalFilter(root, ev.getFilter(), OPERATION.AND, node);
                } else if (node instanceof LogicalOrExpression) {
                    root = this.buildLogicalFilter(root, ev.getFilter(), OPERATION.OR, node);
                } else {
                    throw new NotSupportException(); // 不会到这一步
                }
            }
        }

        this.filter = root;
    }

    @Override
    public void visit(SubqueryFactor node) {
        MySqlSelectVisitor v = new MySqlSelectVisitor();
        node.accept(v);
        tableNode = v.getTableNode();
        tableNode.setSubQuery(true);
        if (node.getAliasUnescapeUppercase() != null) {
            if (tableNode.getAlias() != null) {
                tableNode.setSubAlias(tableNode.getAlias());// 内部子查询的别名
            }

            tableNode.alias(node.getAliasUnescapeUppercase());
        }
    }

    @Override
    public void visit(TableRefFactor node) {
        TableNode table = new TableNode(node.getTable().getIdTextUpUnescape());
        if (node.getAliasUnescapeUppercase() != null) {
            table.alias(node.getAliasUnescapeUppercase());
        }

        this.tableNode = table;
    }

    @Override
    public void visit(CaseWhenOperatorExpression node) {
        throw new NotSupportException("CaseWhenOperatorExpression");
    }

    @Override
    public void visit(ExistsPrimary node) {
        throw new NotSupportException("ExistsPrimary");
    }

    @Override
    public void visit(MatchExpression node) {
        throw new NotSupportException("MatchExpression");
    }

    @Override
    public void visit(IndexHint node) {
        throw new NotSupportException("IndexHint");
    }

    @Override
    public void visit(Dual dual) {
        throw new NotSupportException("Dual");
    }

    // ================== helper =====================

    public static MySqlExprVisitor parser(String condition) {
        if (StringUtils.isEmpty(condition)) {
            return null;
        }

        condition = condition.toUpperCase();
        try {
            // 调用cobar的expression解析器
            MySQLExprParser expr = new MySQLExprParser(new MySQLLexer(condition));
            Expression expression = expr.expression();
            // 反射调用visit构造IFilter
            MySqlExprVisitor parser = new MySqlExprVisitor();
            Class args = expression.getClass();
            Method method = null;
            while (true) {
                method = MethodUtils.getAccessibleMethod(parser.getClass(), "visit", args);
                if (method == null) {
                    args = args.getSuperclass();
                    if (args == null) {
                        throw new NotSupportException("parse failed : " + condition);
                    }
                } else {
                    break;
                }
            }
            method.invoke(parser, expression);
            return parser;
        } catch (Exception e) {
            throw new OptimizerException(e);
        }
    }

    public IBooleanFilter buildBooleanFilter(Object column, Object value, OPERATION operation, Expression exp) {
        IBooleanFilter ibf = ASTNodeFactory.getInstance().createBooleanFilter();
        ibf.setColumn(column);
        ibf.setValue(value);
        ibf.setOperation(operation);
        if (exp != null) {
            ibf.setColumnName(getSqlExprStr(exp)); // 比如将count(*)做为columnName
        }
        return ibf;
    }

    public ILogicalFilter buildLogicalFilter(IFilter column, IFilter value, OPERATION operation, Expression exp) {
        ILogicalFilter ibf = ASTNodeFactory.getInstance().createLogicalFilter();
        ibf.addSubFilter(column);
        ibf.addSubFilter(value);
        ibf.setOperation(operation);
        if (exp != null) {
            ibf.setColumnName(getSqlExprStr(exp));
        }
        return ibf;
    }

    public IBooleanFilter buildConstanctFilter(Comparable constant) {
        IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
        f.setOperation(OPERATION.CONSTANT);
        f.setColumn(constant);
        f.setColumnName(ObjectUtils.toString(constant));
        return f;
    }

    protected boolean eval(Expression expr) {
        Object value = expr.evaluation(emptyMap);
        if (value != null && value != Expression.UNEVALUATABLE) {
            this.columnOrValue = (Comparable) value;
            return true;
        }

        return false;
    }

    private void handleBooleanFilter(BinaryOperatorExpression node, OPERATION op) {
        MySqlExprVisitor lv = new MySqlExprVisitor();
        node.getLeftOprand().accept(lv);

        if (node.getRightOprand() instanceof DMLSelectStatement) {
            // 处理id = (subquery)
            MySqlSelectVisitor rv = new MySqlSelectVisitor();
            node.getRightOprand().accept(rv);
            this.filter = buildBooleanFilter(lv.getColumnOrValue(), rv.getTableNode(), op, node);
        } else {
            MySqlExprVisitor rv = new MySqlExprVisitor();
            node.getRightOprand().accept(rv);
            this.filter = buildBooleanFilter(lv.getColumnOrValue(), rv.getColumnOrValue(), op, node);
        }
    }

    private void handleArithmetric(BinaryOperatorExpression expr, String functionName) {
        IFunction func = ASTNodeFactory.getInstance().createFunction();
        func.setFunctionName(functionName);

        List<Object> args = new ArrayList<Object>(2);
        MySqlExprVisitor leftevi = new MySqlExprVisitor();
        expr.getLeftOprand().accept(leftevi);
        args.add(leftevi.getColumnOrValue());

        MySqlExprVisitor rightevi = new MySqlExprVisitor();
        expr.getRightOprand().accept(rightevi);
        args.add(rightevi.getColumnOrValue());

        func.setArgs(args); // 设置参数
        func.setColumnName(getSqlExprStr(expr));
        this.columnOrValue = func;
    }

    private void handleInExpression(InExpression node) {
        MySqlExprVisitor left = new MySqlExprVisitor();
        node.getLeftOprand().accept(left);
        Object leftColumn = left.getColumnOrValue();
        IBooleanFilter filter = buildBooleanFilter(leftColumn, null, OPERATION.IN, node);

        // 构建values参数
        Expression ex = node.getRightOprand();
        if (ex instanceof InExpressionList) {
            List<Expression> elist = ((InExpressionList) ex).getList();
            List<Object> values = new ArrayList<Object>();
            for (Expression expr : elist) {
                MySqlExprVisitor v = new MySqlExprVisitor();
                expr.accept(v);
                values.add(v.getColumnOrValue());
            }
            filter.setValues(values);
        } else if (ex instanceof QueryExpression) {
            MySqlSelectVisitor v = new MySqlSelectVisitor();
            ex.accept(v);
            filter.setValues(Arrays.asList((Object) v.getTableNode()));
        }

        this.filter = filter;
        if (node.isNot()) {// not in
            filter.setIsNot(true);
        }
    }

    /**
     * get function arg is distinct
     * 
     * @param node
     * @return
     */
    private boolean isDistinct(FunctionExpression node) {
        if (node instanceof Avg) {
            return ((Avg) node).isDistinct();
        } else if (node instanceof Max) {
            return ((Max) node).isDistinct();
        } else if (node instanceof Min) {
            return ((Min) node).isDistinct();
        } else if (node instanceof Count) {
            return ((Count) node).isDistinct();
        } else if (node instanceof Sum) {
            return ((Sum) node).isDistinct();
            // if have other function support distinct,add here
        } else {
            return false;
        }
    }

    private JoinNode commonJoin(TableReference ltable, TableReference rtable) {
        MySqlExprVisitor lv = new MySqlExprVisitor();
        ltable.accept(lv);
        QueryTreeNode left = lv.getTableNode();

        MySqlExprVisitor rv = new MySqlExprVisitor();
        rtable.accept(rv);
        QueryTreeNode right = rv.getTableNode();
        JoinNode joinNode = left.join(right);
        return joinNode;
    }

    private void addJoinOnColumns(IFilter ifilter, JoinNode joinNode) {
        if (ifilter instanceof IBooleanFilter) {
            joinNode.addJoinFilter((IBooleanFilter) ifilter);

        } else if (ifilter instanceof ILogicalFilter) {
            ILogicalFilter ilf = (ILogicalFilter) ifilter;
            if (!ilf.getOperation().equals(OPERATION.AND)) {
                // 比如出现 A.id = B.id And ( A.id = 1 or A.name = 3)
                // 这里的or条件可直接做为other join filter
                // 如果出现 A.id = B.id OR A.name = B.name，那就是一个未知情况了
                joinNode.setOtherJoinOnFilter(ilf);
            } else {
                List<IFilter> subFilter = ilf.getSubFilter();
                if (subFilter != null) {
                    for (IFilter one : subFilter) {
                        addJoinOnColumns(one, joinNode);
                    }
                } else {
                    throw new IllegalStateException("and has no other columns , " + ifilter);
                }
            }
        }
    }

    private IFilter getUsingFilter(List<String> using) {
        IFilter ifilter = null;
        if (using.size() == 1) {
            IColumn column1 = ASTNodeFactory.getInstance().createColumn();
            column1.setColumnName(using.get(0).toUpperCase());

            IColumn column2 = ASTNodeFactory.getInstance().createColumn();
            column2.setColumnName(using.get(0).toUpperCase());
            ifilter = this.buildBooleanFilter(column1, column2, OPERATION.EQ, null);
            ifilter.setColumnName(column1.getColumnName() + "=" + column2.getColumnName());
        } else {
            ILogicalFilter temp = ASTNodeFactory.getInstance().createLogicalFilter();
            temp.setOperation(OPERATION.AND);
            for (String us : using) {
                IColumn column1 = ASTNodeFactory.getInstance().createColumn();
                column1.setColumnName(us.toUpperCase());

                IColumn column2 = ASTNodeFactory.getInstance().createColumn();
                column2.setColumnName(us.toUpperCase());
                IFilter subFilter = this.buildBooleanFilter(column1, column2, OPERATION.EQ, null);
                subFilter.setColumnName(column1.getColumnName() + "=" + column2.getColumnName());
                temp.addSubFilter(subFilter);
            }
            ifilter = temp;
        }

        return ifilter;
    }

    /**
     * get the whole expression string,include everything of this node
     */
    protected String getSqlExprStr(Expression expr) {
        StringBuilder str = new StringBuilder();
        MySQLOutputASTVisitor oa = new MySQLOutputASTVisitor(str);
        expr.accept(oa);
        return str.toString();
    }

    // ================== getter =====================

    public Comparable getColumnOrValue() {
        if (columnOrValue == null) {
            return filter;
        } else {
            return columnOrValue;
        }
    }

    public Comparable getValueForLike() {
        return valueForLike;
    }

    public IFilter getFilter() {
        return filter;
    }

    public QueryTreeNode getTableNode() {
        return tableNode;
    }
}
