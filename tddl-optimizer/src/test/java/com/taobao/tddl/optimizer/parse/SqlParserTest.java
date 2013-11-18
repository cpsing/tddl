package com.taobao.tddl.optimizer.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.cobar.CobarSqlParseManager;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * @author jianghang 2013-11-15 下午3:55:47
 * @since 5.1.0
 */
public class SqlParserTest {

    private static SqlParseManager manager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        manager = new CobarSqlParseManager();
    }

    @Test
    public void testQueryNoCondition() throws SqlParserException, QueryException {

    }

    @Test
    public void testQueryOnPrimaryWithIndex() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE ID=1";
        QueryTreeNode qn = query(sql);

        QueryTreeNode qnExpected = new TableNode("TABLE1").select("*").query("ID=1");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryOnJoinNormal() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1")
            .select("*")
            .setAllWhereFilter(FilterUtils.createFilter("A.NAME=1"));
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryOnJoinInner() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setInnerJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1")
            .select("*")
            .setAllWhereFilter(FilterUtils.createFilter("A.NAME=1"));
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryOnJoinUsingEquals_left_outer() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A LEFT JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setLeftOuterJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1")
            .select("*")
            .setAllWhereFilter(FilterUtils.createFilter("A.NAME=1"));
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryOnJoinUsingEquals_right_outer() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A RIGHT JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setRightOuterJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1")
            .select("*")
            .setAllWhereFilter(FilterUtils.createFilter("A.NAME=1"));
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryOnJoinUsingEquals_twoArgs() throws SqlParserException, QueryException {

        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID AND A.NAME = B.NAME WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .query("A.NAME=1")
            .select("*")
            .setAllWhereFilter(FilterUtils.createFilter("A.NAME=1"));
        Assert.assertEquals(qnExpected.toString(), qn.toString());

    }

    // @Test
    public void testQueryOnJoinUsingEquals_or_will_throw_excps() throws Exception {
        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID OR A.NAME = B.NAME WHERE A.NAME=1";
        try {
            QueryTreeNode qn = query(sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("java.lang.IllegalArgumentException: not support 'or' in join on statment ",
                e.getCause().getMessage());
        }
    }

    // @Test
    public void testQueryUsingPageOptmizer_case1() throws Exception {
        String sql = " select * from table1 where id >= (select id from table1 limit 100000,1) limit 100;";
        QueryTreeNode qn;
        try {
            qn = query(sql);
            qn.toString();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("java.lang.RuntimeException: boolean filter right value not support subquery.",
                e.getCause().getMessage());
        }

    }

    // @Test
    public void testQueryUsingPageOptmizer_case2() throws Exception {
        String sql = " SELECT A.ID,A.NAME FROM (SELECT * FROM TABLE1 WHERE ID > 100 LIMIT 10000,10) AS A,TABLE2 B WHERE A.ID = B.ID;";
        QueryTreeNode qn;
        qn = query(sql);

        TableNode table1 = new TableNode("table1");
        TableNode table2 = new TableNode("table2");
        QueryTreeNode qnExpected = table1.select("*")
            .query("ID>100")
            .setLimitFrom(10000)
            .setLimitTo(10)
            .alias("A")
            .select("A.ID,A.NAME")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .select("A.ID,A.NAME");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    // @Test
    public void testQueryUsingPageOptmizer_case3() throws Exception {
        String sql = "SELECT A.ID,A.NAME FROM (SELECT * FROM TABLE1 WHERE ID > 100 LIMIT 10000,10) AS A,TABLE2 B WHERE A.ID = B.ID AND A.NAME=B.NAME;";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.select("*")
            .query("ID>100")
            .setLimitFrom(10000)
            .setLimitTo(10)
            .alias("A")
            .select("A.ID,A.NAME")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .select("A.ID,A.NAME");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrderBy() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrderByIDASC() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID ASC";

        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", true);
        Assert.assertEquals(qnExpected.toString(), qn.toString());

    }

    @Test
    public void testQueryWithOrderByDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID DESC";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", false);
        Assert.assertEquals(qnExpected.toString(), qn.toString());

    }

    public void testQueryWithOrderByIdAndNameASC() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID,NAME";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID").orderBy("NAME");
        Assert.assertEquals(qnExpected.toString(), qn.toString());

    }

    @Test
    public void testQueryWithOrderByIdAndNameDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID,NAME DESC";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID").orderBy("NAME", false);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrderByIdDescAndNameDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID DESC,NAME DESC";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", false).orderBy("NAME", false);
        Assert.assertEquals(qnExpected.toString(), qn.toString());

    }

    @Test
    public void testQueryWithOrExpression() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE NAME = 2323 OR ID=1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").query("NAME=2323 || ID=1");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryComplex() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE (SCHOOL=1 OR NAME=2) AND (ID=1)";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").query("(SCHOOL=1 || NAME=2) && (ID=1)");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testJoinWithPrimary() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1,TABLE2 WHERE TABLE1.NAME=1 AND TABLE1.ID=TABLE2.ID";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.join("TABLE2")
            .query("TABLE1.NAME=1 AND TABLE1.ID=TABLE2.ID")
            .setAllWhereFilter(FilterUtils.createFilter("TABLE1.NAME=1 AND TABLE1.ID=TABLE2.ID"))
            .select("*");

        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testJoinWithPrimaryAlias() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1,TABLE2 T2 WHERE T1.NAME=1 AND T1.ID=T2.ID";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("T1")
            .join(table2.alias("T2"))
            .query("T1.NAME=1 AND T1.ID=T2.ID")
            .setAllWhereFilter(FilterUtils.createFilter("T1.NAME=1 AND T1.ID=T2.ID"))
            .select("*");

        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithAndExpression() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE T1.ID=4 AND T1.ID>=2";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").alias("T1").query("T1.ID=4 && T1.ID>=2");

        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithAndExpression2() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID<=10 AND ID>=5";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query("ID<=10 && ID>=5");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithAndExpression3() throws SqlParserException, QueryException {
        String sql = "select * from table1 t1 where name='4' and id<=2";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(" NAME=4 && ID<=2");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrMutiExpression1() throws SqlParserException, QueryException {
        String sql = "select * from table1 t1 where id<5 or id<=6 or id=3";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(" ID<5 || ID<=6 || ID=3");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrMutiExpression2() throws SqlParserException, QueryException {
        String sql = "select * from table1 t1 where id<5 or id<=6 or id=7";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(" ID<5 || ID<=6 || ID=7");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(*) FROM TABLE1 T1 WHERE ID = 1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("COUNT");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("*");

        List args = new ArrayList();
        args.add(c);

        f.setArgs(args);
        f.setTableName("TABLE1");
        f.setColumnName("COUNT(*)");
        QueryTreeNode qnExpected = table1.alias("T1").query("ID=1").select(f);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction1() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(ID) FROM TABLE1 T1 WHERE ID = 1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("COUNT");
        f.setColumnName("COUNT(ID)");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        List args = new ArrayList();
        args.add(c);
        f.setArgs(args);
        QueryTreeNode qnExpected = table1.alias("T1").query("ID=1").select(f);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction_double_function() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(ID),AVG(ID) FROM TABLE1 T1 WHERE ID = 1";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("AVG");
        f.setColumnName("AVG(ID)");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        List args = new ArrayList();
        args.add(c);
        f.setArgs(args);

        IFunction f2 = ASTNodeFactory.getInstance().createFunction();
        f2.setFunctionName("COUNT");
        f2.setColumnName("COUNT(ID)");
        args = new ArrayList();
        args.add(c);
        f2.setArgs(args);

        QueryTreeNode qnExpected = table1.alias("T1").query(" ID=1").select(f2, f);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    // 语法错误
    @Test
    public void testFunction_to_char() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE DATE_ADD(ID, INTERVAL 1 SECOND)= '2012-11-11'";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("DATE_ADD");

        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");
        List args = new ArrayList();
        args.add(c);
        args.add("INTERVAL 1 SECOND");
        f.setArgs(args);

        f.setColumnName("DATE_ADD(ID, INTERVAL 1 SECOND)");
        IFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setOperation(OPERATION.EQ);
        ((IBooleanFilter) filter).setColumn(f);
        ((IBooleanFilter) filter).setValue("2012-11-11");

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(filter);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction_twoArgs() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE IFNULL(STR_TO_DATE(ID, '%d,%m,%y'),1) = '1'";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("STR_TO_DATE");

        IFunction f2 = ASTNodeFactory.getInstance().createFunction();
        f2.setFunctionName("IFNULL");
        f2.setColumnName("IFNULL(STR_TO_DATE(ID, '%d,%m,%y'), 1)");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        List args = new ArrayList();
        args.add(c);
        args.add("%d,%m,%y");
        f.setArgs(args);

        f.setColumnName("STR_TO_DATE(id, '%d,%m,%Y')");
        args = new ArrayList();
        args.add(f);
        args.add(1);

        f2.setArgs(args);
        IFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setOperation(OPERATION.EQ);
        ((IBooleanFilter) filter).setColumn(f2);
        ((IBooleanFilter) filter).setValue('1');

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(filter);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction_noArgs() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID = NOW()";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("NOW");
        f.setColumnName("NOW()");

        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        IFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setOperation(OPERATION.EQ);
        ((IBooleanFilter) filter).setColumn(c);
        ((IBooleanFilter) filter).setValue(f);

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(filter);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction_noArgs_with_other() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID = NOW() AND NAME=1";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("NOW");
        f.setColumnName("NOW()");

        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        IColumn c2 = ASTNodeFactory.getInstance().createColumn();
        c2.setColumnName("NAME");

        IFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setOperation(OPERATION.EQ);
        ((IBooleanFilter) filter).setColumn(c);
        ((IBooleanFilter) filter).setValue(f);

        IFilter filter2 = ASTNodeFactory.getInstance().createBooleanFilter();
        filter2.setOperation(OPERATION.EQ);
        ((IBooleanFilter) filter2).setColumn(c2);
        ((IBooleanFilter) filter2).setValue(1);

        IFilter filter3 = ASTNodeFactory.getInstance().createLogicalFilter();
        filter3.setOperation(OPERATION.AND);
        ((ILogicalFilter) filter3).addSubFilter(filter);
        ((ILogicalFilter) filter3).addSubFilter(filter2);

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(filter3);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testFunction_timeFunction() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID >= STR_TO_DATE('1900-10-10','%d,%m,%Y')";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("STR_TO_DATE");

        List args = new ArrayList();
        args.add("1900-10-10");
        args.add("%d,%m,%Y");
        f.setArgs(args);

        f.setColumnName("STR_TO_DATE('1900-10-10', '%d,%m,%Y')");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        IFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setOperation(OPERATION.GT_EQ);
        ((IBooleanFilter) filter).setColumn(c);
        ((IBooleanFilter) filter).setValue(f);

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(filter);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testGroupBY() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID = 1 GROUP BY NAME ";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");

        QueryTreeNode qnExpected = table1.select("*").alias("T1").query(" ID=1").groupBy("NAME");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    // @Test
    public void testFunction_timeFunction1() throws Exception {
        // TODO shenxun :这个compare的函数要想办法弄通顺
        String sql = "select * from table1 t1 where TO_DAYS(NOW()) + TO_DAYS(id) <= 30";
        QueryTreeNode qn = query(sql);
        // IQuery query = (IQuery) o.optimizeAndAssignment(qn, null, null);
        //
        // validateIQuery(query,
        // null,
        // "TABLE1._ID",
        // "T1",
        // "BooleanFilter [column=-(TO_DAYS(NOW()),TO_DAYS(T1.ID)), value=30, operation=5]");
    }

    // @Test
    public void testFunction_timeFunction3() throws Exception {
        // TODO shenxun :这个compare的函数要想办法弄通顺
        // String sql = "select id from table1 t1 where id=+1";
        // QueryTreeNode qn = query(sql);
        // System.out.println(qn);
        // IQuery query = (IQuery) o.optimizeAndAssignment(qn, null, null);
        //
        // validateIQuery(query,
        // null,
        // "TABLE1._ID",
        // "T1",
        // "BooleanFilter [column=-(TO_DAYS(NOW()),TO_DAYS(T1.ID)), value=30, operation=5]");
    }

    // @Test
    // public void testFunction_timeFunction2() throws Exception{
    // //TODO shenxun :这个compare的函数要想办法弄通顺
    // String sql =
    // "select * from table1 t1 where DATE_SUB('1998-01-02' - INTERVAL 1 SECOND) <= '30'";
    // QueryTreeNode qn = query(sql);
    // qnExpected.build();Assert.assertEquals(/*using
    // com.taobao.ustore.sqlparser.test.MatchUtils
    // to format */
    // "Query [lockModel=SHARED_LOCK, indexKey=TABLE1._ID, "
    // +"resultSetFilter=BooleanFilter [column=Function [functionName=SUBTRACTION, args=[Function [functionName=TO_DAYS, args=[Function [functionName=NOW, args=[], distinct=false, ]], distinct=false, ], Function [functionName=TO_DAYS, args=[Column [name=ID, tableName=T1, dataType=LONG_VAL, distinct=false, ]], distinct=false, ]], distinct=false, ], value=30, operation=5], "
    // +"orderBy=[], "
    // +"columns=[Column [name=NAME, tableName=T1, dataType=STRING_VAL, distinct=false, ], Column [name=SCHOOL, tableName=T1, dataType=STRING_VAL, distinct=false, ], Column [name=ID, tableName=T1, dataType=LONG_VAL, distinct=false, ]], alias=T1, "
    // +"queryConcurrency=SEQUENTIAL, consistentRead=true]",
    // o.optimize(qn,null,null).toString());
    // }

    @Test
    public void testUpdate() throws SqlParserException, QueryException {
        String sql = "UPDATE TABLE1 SET NAME=2 WHERE ID>=5 AND ID<=5";
        UpdateNode un = update(sql);

        TableNode table1 = new TableNode("TABLE1");
        UpdateNode unExpected = table1.update("NAME", new Comparable[] { 2 });
        unExpected.setNode(table1.query("ID>=5 AND ID<=5"));

        Assert.assertEquals(unExpected.toString(), un.toString());
    }

    @Test
    public void testDelete() throws SqlParserException, QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID>=5 AND ID<=5";
        DeleteNode dn = delete(sql);

        TableNode table1 = new TableNode("TABLE1");
        DeleteNode dnExpected = table1.delete();
        dnExpected.setNode(table1.query("ID>=5 AND ID<=5"));

        Assert.assertEquals(dnExpected.toString(), dn.toString());
    }

    @Test
    public void testInsert() throws SqlParserException, QueryException {
        String sql = "INSERT INTO TABLE1(ID) VALUES (2)";
        InsertNode in = insert(sql);

        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID", new Comparable[] { 2 });

        Assert.assertEquals(inExpected.toString(), in.toString());
    }

    @Test
    public void testInsert2() throws SqlParserException, QueryException {
        String sql = "INSERT INTO TABLE1(ID, NAME1, NAME2) VALUES (2, 'sun', 'sysu')";

        InsertNode in = insert(sql);

        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID NAME1 NAME2", new Comparable[] { 2, "sun", "sysu" });

        Assert.assertEquals(inExpected.toString(), in.toString());
    }

    @Test
    public void testLimit() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 LIMIT 1,10";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").limit(1, 10);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testLimit1() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE ID = 10 LIMIT 10";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").query("id=10").setLimitFrom(0).setLimitTo(10);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testLimit_bindval1() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE TABLE1.ID = 10 LIMIT ?,10";
        QueryTreeNode qn = query(sql);

        Assert.assertTrue(qn.getLimitFrom() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitFrom();

        Assert.assertEquals(0, bv.getBindVal());
        Assert.assertEquals(10L, qn.getLimitTo());
    }

    @Test
    public void testLimit_bindval2() throws SqlParserException, QueryException {
        String sql = "select * from table1 where table1.id = 10 limit 1,?";
        QueryTreeNode qn = query(sql);
        Assert.assertTrue(qn.getLimitTo() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitTo();

        Assert.assertEquals(0, bv.getBindVal());
        Assert.assertEquals(1L, qn.getLimitFrom());
    }

    @Test
    public void testLimit_bindval3() throws SqlParserException, QueryException {
        String sql = "select * from table1 where table1.id = 10 limit ?,?";
        QueryTreeNode qn = query(sql);
        Assert.assertTrue(qn.getLimitFrom() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitFrom();

        Assert.assertEquals(0, bv.getBindVal());

        Assert.assertTrue(qn.getLimitTo() instanceof BindVal);
        bv = (BindVal) qn.getLimitTo();

        Assert.assertEquals(1, bv.getBindVal());

    }

    @Test
    public void testPreparedInsertSql() throws SqlParserException, QueryException {
        String sql = "INSERT INTO TABLE1(ID,NAME,SCHOOL) VALUES (?, ?, ?)";
        InsertNode in = insert(sql);
        Map<Integer, ParameterContext> parameterSettings = null;
        parameterSettings = new TreeMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 0, 2 });
        ParameterContext p2 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "sun" });
        ParameterContext p3 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, "sysu" });
        parameterSettings.put(0, p1);
        parameterSettings.put(1, p2);
        parameterSettings.put(2, p3);

        in.assignment(parameterSettings);
        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID NAME SCHOOL", new Comparable[] { 2, "sun", "sysu" });

        Assert.assertEquals(inExpected.toString(), in.toString());
    }

    @Test
    public void testPreparedUpdateSql() throws SqlParserException, QueryException {
        String sql = "UPDATE TABLE1 SET ID=? WHERE ID>=? AND ID<=?";
        UpdateNode un = update(sql);

        Map<Integer, ParameterContext> parameterSettings = null;
        parameterSettings = new TreeMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 0, 2 });
        ParameterContext p2 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 3 });
        ParameterContext p3 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 5 });
        parameterSettings.put(0, p1);
        parameterSettings.put(1, p2);
        parameterSettings.put(2, p3);

        un.assignment(parameterSettings);

        TableNode table1 = new TableNode("TABLE1");
        UpdateNode unExpected = table1.update("ID", new Comparable[] { 2 });
        unExpected.setNode(table1.query("ID>=3 AND ID<=5"));

        Assert.assertEquals(unExpected.toString(), un.toString());
    }

    @Test
    public void testPreparedDeleteSql() throws SqlParserException, QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID>=? AND ID<=?";
        DeleteNode dn = delete(sql);

        Map<Integer, ParameterContext> parameterSettings = null;
        parameterSettings = new TreeMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 0, 3 });
        ParameterContext p2 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 });
        parameterSettings.put(0, p1);
        parameterSettings.put(1, p2);

        dn.assignment(parameterSettings);
        TableNode table1 = new TableNode("TABLE1");
        DeleteNode dnExpected = table1.delete();
        dnExpected.setNode(table1.query("ID>=3 AND ID<=5"));
        Assert.assertEquals(dnExpected.toString(), dn.toString());
    }

    // @Test
    public void testJoinRestrictionCopy() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.ID>1+4 AND B.ID<12-1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("ID", "ID")
            .query("A.ID>5 && B.ID<11");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testDistinct() throws SqlParserException, QueryException {
        String sql = "SELECT  COUNT(DISTINCT ID) FROM TABLE1";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");
        c.setDistinct(true);

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("COUNT");
        f.setColumnName("COUNT(DISTINCT ID)");

        List args = new ArrayList();
        args.add(c);

        f.setArgs(args);

        QueryTreeNode qnExpected = table1.select(f);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithOrderByFunction() throws Exception {
        String sql = "SELECT * FROM TABLE1 WHERE ID=1 ORDER BY COUNT(ID)";

        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("COUNT");
        List args = new ArrayList();
        args.add(c);

        f.setArgs(args);

        f.setColumnName("COUNT(ID)");
        QueryTreeNode qnExpected = table1.select("*").query("ID=1").orderBy(f, true);
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testQueryWithLike() throws SqlParserException, QueryException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME LIKE '%XASX%'";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME LIKE '%XASX%'");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testMultiAnd() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? AND (ID>? AND ID<?)";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? AND ID>? AND ID<?");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testMultiOr() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? OR(ID>? OR ID<?)";
        QueryTreeNode qn = query(sql);
        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? OR(ID>? OR ID<?)");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    @Test
    public void testMultiAndOr() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? AND NAME>? AND (ID=? OR ID<?)";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? AND NAME>? AND (ID=? OR ID<?)");
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    // @Test
    public void testWhereSubQuery() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=(SELECT NAME FROM TABLE2 B WHERE B.ID=1)";
        QueryTreeNode qn = query(sql);

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME");

        TableNode table2 = new TableNode("TABLE2");
        table2.alias("B").select("NAME").query("B.ID=1");
        IBooleanFilter filter = ASTNodeFactory.getInstance().createBooleanFilter();
        filter.setColumn("NAME");
        filter.setOperation(OPERATION.EQ);
        filter.setValue(new QueryNode(table2));

        table1.query(filter);

        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    // ==================================================

    private QueryTreeNode query(String sql) throws SqlParserException {
        SqlAnalysisResult sm = manager.parse(sql, false);
        QueryTreeNode qn = null;
        if (sm.getSqlType() == SqlType.SELECT) {
            qn = sm.getQueryTreeNode(null);
        } else {
            qn = new KVIndexNode(null);
            qn.setSql(sql);
        }
        return qn;
    }

    private UpdateNode update(String sql) throws SqlParserException {
        SqlAnalysisResult sm = manager.parse(sql, false);
        UpdateNode qn = null;
        if (sm.getSqlType() == SqlType.UPDATE) {
            qn = sm.getUpdateNode(null);
        }

        return qn;
    }

    private DeleteNode delete(String sql) throws SqlParserException {
        SqlAnalysisResult sm = manager.parse(sql, false);
        DeleteNode qn = null;
        if (sm.getSqlType() == SqlType.DELETE) {
            qn = sm.getDeleteNode(null);
        }

        return qn;
    }

    private InsertNode insert(String sql) throws SqlParserException {
        SqlAnalysisResult sm = manager.parse(sql, false);
        InsertNode qn = null;
        if (sm.getSqlType() == SqlType.INSERT) {
            qn = sm.getInsertNode(null);
        }

        return qn;
    }

    public static Map<Integer, ParameterContext> convert(List<Object> args) {
        Map<Integer, ParameterContext> map = new HashMap<Integer, ParameterContext>(args.size());
        int index = 0;
        for (Object obj : args) {
            ParameterContext context = new ParameterContext(ParameterMethod.setObject1, new Object[] { index, obj });
            map.put(index, context);
            index++;
        }
        return map;
    }

    public static Map<Integer, ParameterContext> convert(Object[] args) {
        return convert(Arrays.asList(args));
    }
}
