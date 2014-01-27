package com.taobao.tddl.optimizer.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author jianghang 2013-11-15 下午3:55:47
 * @since 5.0.0
 */
public class SqlParserTest extends BaseOptimizerTest {

    private static final Logger logger = LoggerFactory.getLogger(SqlParserTest.class);

    @Test
    public void testQuery_简单主键查询() throws SqlParserException, QueryException {
        String sql = "select * from table1 where id=1 or id = 2";
        QueryTreeNode qn = query(sql);
        qn.build();

        QueryTreeNode qnExpected = new TableNode("TABLE1").query("ID=1 or ID=2");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_字段别名() throws SqlParserException, QueryException {
        String sql = "SELECT ID AS TID,NAME,SCHOOL FROM TABLE1  WHERE ID=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        QueryTreeNode qnExpected = new TableNode("TABLE1").select("ID AS TID,NAME,SCHOOL").query("ID=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_字段别名_表别名() throws SqlParserException, QueryException {
        String sql = "SELECT T.ID AS TID,T.NAME,T.SCHOOL FROM TABLE1 T  WHERE ID=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        QueryTreeNode qnExpected = new TableNode("TABLE1").alias("T").select("ID AS TID,NAME,SCHOOL").query("ID=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_函数别名() throws SqlParserException, QueryException {
        String sql = "SELECT T.ID , LENGTH(NAME) AS LEN FROM TABLE1 T  WHERE ID=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        IFunction function = ASTNodeFactory.getInstance().createFunction();
        function.setColumnName("LENGTH(NAME)");
        function.setAlias("LEN");
        function.setFunctionName("LENGTH");

        QueryTreeNode qnExpected = new TableNode("TABLE1").alias("T").select("T.ID").query("ID=1");
        qnExpected.addColumnsSelected(function);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_普通join() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_内连接() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setInnerJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_左连接() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A LEFT JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setLeftOuterJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_右连接() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A RIGHT JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setRightOuterJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Ignore("mysql parser语法上暂时不支持，需要修改")
    @Test
    public void testQuery_outter连接() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A OUTER JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .setOuterJoin()
            .addJoinKeys("A.ID", "B.ID")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_普通链接_多个连接条件() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID AND A.NAME = B.NAME WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .query("A.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_普通链接_字段别名() throws SqlParserException, QueryException {
        String sql = "SELECT A.ID AS AID,A.SCHOOL AS ASCHOOL,B.* FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID AND A.NAME = B.NAME WHERE A.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .query("A.NAME=1")
            .select("A.ID AS AID,A.SCHOOL AS ASCHOOL,B.*");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_普通链接_order_group_having() throws SqlParserException, QueryException {
        String sql = "SELECT A.ID AS AID,A.SCHOOL AS ASCHOOL,B.* FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID AND A.NAME = B.NAME WHERE A.NAME=1";
        sql += " GROUP BY AID HAVING AID > 0 ORDER BY A.ID ASC ";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .query("A.NAME=1")
            .select("A.ID AS AID,A.SCHOOL AS ASCHOOL,B.*")
            .groupBy("AID")
            .having("AID > 0")
            .orderBy("A.ID", true);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_普通链接_函数() throws SqlParserException, QueryException {
        String sql = "SELECT A.ID as AID,A.ID,COUNT(A.ID),COUNT(*) FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID AND A.NAME = B.NAME WHERE A.NAME=1";
        sql += " GROUP BY AID HAVING AID > 0 ORDER BY A.ID ASC ";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("A.ID", "B.ID")
            .addJoinKeys("A.NAME", "B.NAME")
            .query("A.NAME=1")
            .select("A.ID AS AID,A.ID")
            .groupBy("AID")
            .having("AID > 0")
            .orderBy("A.ID", true);
        IFunction function1 = ASTNodeFactory.getInstance().createFunction();
        function1.setColumnName("COUNT(A.ID)");
        function1.setFunctionName("COUNT");

        IFunction function2 = ASTNodeFactory.getInstance().createFunction();
        function2.setColumnName("COUNT(*)");
        function2.setFunctionName("COUNT");
        qnExpected.addColumnsSelected(function1);
        qnExpected.addColumnsSelected(function2);

        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    // @Test
    // 后续调整
    public void testQuery_内连接_OR条件_不支持() throws Exception {
        String sql = "SELECT * FROM TABLE1 A INNER JOIN TABLE2 B ON A.ID=B.ID OR A.NAME = B.NAME WHERE A.NAME=1";
        try {
            QueryTreeNode qn = query(sql);
            qn.build();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("java.lang.IllegalArgumentException: not support 'or' in join on statment ",
                e.getCause().getMessage());
        }
    }

    @Test
    public void testQuery_多表join() throws Exception {
        String sql = "SELECT * FROM TABLE1 JOIN TABLE2 LEFT JOIN TABLE3 ON (TABLE3.ID=TABLE2.ID) LEFT JOIN TABLE4 ON (TABLE4.ID=TABLE1.ID) WHERE TABLE2.ID= TABLE4.ID";
        QueryTreeNode qn = query(sql);
        qn.build();

        Assert.assertTrue(qn instanceof JoinNode);
    }

    @Test
    public void testQuery_正常orderby() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_OrderByAsc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID ASC";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", true);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_OrderByDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID DESC";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", false);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    public void testQuery_OrderByIdAndNameASC() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID,NAME";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID").orderBy("NAME");
        qnExpected.build();
        assertEquals(qn, qnExpected);

    }

    @Test
    public void testQuery_OrderByIdAndNameDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID,NAME DESC";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID").orderBy("NAME", false);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_OrderByIdDescAndNameDesc() throws Exception {
        String sql = "SELECT ID,NAME FROM TABLE1 WHERE ID=1 ORDER BY ID DESC,NAME DESC";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("ID,NAME").query("ID=1").orderBy("ID", false).orderBy("NAME", false);
        qnExpected.build();
        assertEquals(qn, qnExpected);

    }

    @Test
    public void testQuery_OrExpression() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE NAME = 2323 OR ID=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.query("NAME=2323 || ID=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_复杂条件() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE (SCHOOL=1 OR NAME=2) AND (ID=1)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.query("(SCHOOL=1 || NAME=2) && (ID=1)");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testJoin_多表主键关联() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1,TABLE2 WHERE TABLE1.NAME=1 AND TABLE1.ID=TABLE2.ID";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.join("TABLE2").query("TABLE1.NAME=1 AND TABLE1.ID=TABLE2.ID");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testJoin_多表主键关联_表别名() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1,TABLE2 WHERE T1.NAME=1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("T1").join(table2).query("T1.NAME=1");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_and表达式() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID<=10 AND ID>=5";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.alias("T1").query("ID<=10 AND ID>=5");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_and表达式_别名() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE T1.ID=4 AND T1.ID>=2";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("*").alias("T1").query("T1.ID=4 && T1.ID>=2");
        qnExpected.build();

        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_and表达式_字符串() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE NAME='4' AND ID<=2";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.alias("T1").query("NAME=4 && ID<=2");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_or表达式() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID<5 OR ID<=6 OR ID=3";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.alias("T1").query("ID<5 || ID<=6 || ID=3");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQueryWith_or表达式_别名() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID<5 OR ID<=6 OR ID=7";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.alias("T1").query(" ID<5 || ID<=6 || ID=7");
        table1.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(*) FROM TABLE1 T1 WHERE ID = 1";
        QueryTreeNode qn = query(sql);
        qn.build();

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
        qnExpected.build();
        assertEquals(qn, qnExpected);
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
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_double_function() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(ID),AVG(ID) FROM TABLE1 T1 WHERE ID = 1";
        QueryTreeNode qn = query(sql);
        qn.build();
        TableNode table1 = new TableNode("TABLE1");

        QueryTreeNode qnExpected = table1.alias("T1").query("ID=1").select("COUNT(ID),AVG(ID)");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_to_char() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE DATE_ADD(ID, INTERVAL 1 SECOND)= '2012-11-11'";
        QueryTreeNode qn = query(sql);
        qn.build();

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

        QueryTreeNode qnExpected = table1.alias("T1").query(filter);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_twoArgs() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE IFNULL(STR_TO_DATE(ID, '%d,%m,%y'),1) = '1'";
        QueryTreeNode qn = query(sql);
        qn.build();

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

        QueryTreeNode qnExpected = table1.alias("T1").query(filter);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_noArgs() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID = NOW()";
        QueryTreeNode qn = query(sql);
        qn.build();
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

        QueryTreeNode qnExpected = table1.alias("T1").query(filter);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testGroupBY() throws Exception {
        String sql = "SELECT * FROM TABLE1 T1 WHERE ID = 1 GROUP BY NAME ";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.alias("T1").query(" ID=1").groupBy("NAME");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_提前计算() throws SqlParserException, QueryException {
        String sql = "SELECT 1+1 FROM TABLE1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("2");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testFunction_提前计算_bindVal() throws SqlParserException, QueryException {
        String sql = "SELECT 1+? FROM TABLE1";
        QueryTreeNode qn = query(sql, Arrays.asList(Integer.valueOf(1)));

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("1+?");// 不做计算，否则解析结果不能缓存
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testJoin_条件提前计算() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.ID>1+4 AND B.ID<12-1";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        QueryTreeNode qnExpected = table1.alias("A")
            .join(table2.alias("B"))
            .addJoinKeys("ID", "ID")
            .query("A.ID>5 && B.ID<11");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testUpdate_正常() throws SqlParserException, QueryException {
        String sql = "UPDATE TABLE1 SET NAME=2 WHERE ID>=5 AND ID<=5";
        UpdateNode un = update(sql);
        un.build();

        TableNode table1 = new TableNode("TABLE1");
        UpdateNode unExpected = table1.update("NAME", new Comparable[] { 2 });
        table1.query("ID>=5 AND ID<=5");
        unExpected.build();
        assertEquals(un, unExpected);
    }

    @Test
    public void testDelete_正常() throws SqlParserException, QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID>=5 AND ID<=5";
        DeleteNode dn = delete(sql);
        dn.build();

        TableNode table1 = new TableNode("TABLE1");
        DeleteNode dnExpected = table1.delete();
        table1.query("ID>=5 AND ID<=5");
        dnExpected.build();

        assertEquals(dn, dnExpected);
    }

    @Test
    public void testInsert_无字段() throws SqlParserException, QueryException {
        String sql = "INSERT INTO TABLE1(ID) VALUES (2)";
        InsertNode in = insert(sql);
        in.build();

        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID", new Comparable[] { 2 });
        inExpected.build();

        assertEquals(in, inExpected);
    }

    @Test
    public void testInsert_多字段() throws SqlParserException, QueryException {
        String sql = "INSERT INTO TABLE1(ID, NAME, SCHOOL) VALUES (2, 'sun', 'sysu')";

        InsertNode in = insert(sql);
        in.build();

        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID NAME SCHOOL", new Comparable[] { 2, "sun", "sysu" });
        inExpected.build();

        assertEquals(in, inExpected);
    }

    @Test
    public void testLimit() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 LIMIT 1,10";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.limit(1, 10);
        qnExpected.build();

        assertEquals(qn, qnExpected);
    }

    @Test
    public void testLimit1() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE ID = 10 LIMIT 10";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.query("id=10").setLimitFrom(0).setLimitTo(10);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testLimit_bindval1() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE TABLE1.ID = 10 LIMIT ?,10";
        QueryTreeNode qn = query(sql);
        qn.build();

        Assert.assertTrue(qn.getLimitFrom() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitFrom();
        Assert.assertEquals(1, bv.getBindVal());
        Assert.assertEquals(10L, qn.getLimitTo());
    }

    @Test
    public void testLimit_bindval2() throws SqlParserException, QueryException {
        String sql = "select * from table1 where table1.id = 10 limit 1,?";
        QueryTreeNode qn = query(sql);
        qn.build();
        Assert.assertTrue(qn.getLimitTo() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitTo();
        Assert.assertEquals(1, bv.getBindVal());
        Assert.assertEquals(1L, qn.getLimitFrom());
    }

    @Test
    public void testLimit_bindval3() throws SqlParserException, QueryException {
        String sql = "select * from table1 where table1.id = 10 limit ?,?";
        QueryTreeNode qn = query(sql);
        qn.build();
        Assert.assertTrue(qn.getLimitFrom() instanceof BindVal);
        BindVal bv = (BindVal) qn.getLimitFrom();
        Assert.assertEquals(1, bv.getBindVal());
        Assert.assertTrue(qn.getLimitTo() instanceof BindVal);
        bv = (BindVal) qn.getLimitTo();
        Assert.assertEquals(2, bv.getBindVal());

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
        parameterSettings.put(1, p1);
        parameterSettings.put(2, p2);
        parameterSettings.put(3, p3);

        in.assignment(parameterSettings);
        in.build();

        TableNode table1 = new TableNode("TABLE1");
        InsertNode inExpected = table1.insert("ID NAME SCHOOL", new Comparable[] { 2, "sun", "sysu" });
        inExpected.build();
        assertEquals(in, inExpected);
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
        parameterSettings.put(1, p1);
        parameterSettings.put(2, p2);
        parameterSettings.put(3, p3);

        un.assignment(parameterSettings);
        un.build();

        TableNode table1 = new TableNode("TABLE1");
        UpdateNode unExpected = table1.update("ID", new Comparable[] { 2 });
        table1.query("ID>=3 AND ID<=5");
        unExpected.build();

        assertEquals(un, unExpected);
    }

    @Test
    public void testPreparedDeleteSql() throws SqlParserException, QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID>=? AND ID<=?";
        DeleteNode dn = delete(sql);

        Map<Integer, ParameterContext> parameterSettings = null;
        parameterSettings = new TreeMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 0, 3 });
        ParameterContext p2 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 });
        parameterSettings.put(1, p1);
        parameterSettings.put(2, p2);

        dn.assignment(parameterSettings);
        dn.build();

        TableNode table1 = new TableNode("TABLE1");
        DeleteNode dnExpected = table1.delete();
        table1.query("ID>=3 AND ID<=5");
        dnExpected.build();
        assertEquals(dn, dnExpected);
    }

    @Test
    public void testDistinct() throws SqlParserException, QueryException {
        String sql = "SELECT COUNT(DISTINCT ID) FROM TABLE1";
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
        qn.build();

        QueryTreeNode qnExpected = table1.select(f);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_orderBy加函数() throws Exception {
        String sql = "SELECT * FROM TABLE1 WHERE ID=1 ORDER BY COUNT(ID)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setColumnName("ID");

        IFunction f = ASTNodeFactory.getInstance().createFunction();
        f.setFunctionName("COUNT");
        List args = new ArrayList();
        args.add(c);

        f.setArgs(args);

        f.setColumnName("COUNT(ID)");
        QueryTreeNode qnExpected = table1.query("ID=1").orderBy(f, true);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testQuery_Like() throws SqlParserException, QueryException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME LIKE '%XASX%'";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME LIKE '%XASX%'");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testMultiAnd() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? AND (ID>? AND ID<?)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? AND ID>? AND ID<?");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testMultiOr() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? OR(ID>? OR ID<?)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? OR(ID>? OR ID<?)");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testMultiAndOr() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=? AND NAME>? AND (ID=? OR ID<?)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME").query("NAME=? AND NAME>? AND (ID=? OR ID<?)");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testWhere_字段子查询() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=(SELECT NAME FROM TABLE2 B WHERE B.ID=1)";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME");

        TableNode table2 = new TableNode("TABLE2");
        table2.alias("B").select("NAME").query("B.ID=1");
        table1.query("NAME=(SELECT NAME FROM TABLE2 B WHERE B.ID=1)");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testWhere_字段多级子查询() throws QueryException, SqlParserException {
        String subSql = "SELECT B.* FROM TABLE2 B WHERE B.ID=1 GROUP BY SCHOOL HAVING COUNT(*) > 1 ORDER BY ID DESC LIMIT 1";
        String sql = "SELECT NAME FROM TABLE1 WHERE NAME=(SELECT C.NAME FROM (" + subSql + ") C )";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        QueryTreeNode qnExpected = table1.select("NAME");

        TableNode table2 = new TableNode("TABLE2");
        table2.alias("C").setSubAlias("B").select("*").query("B.ID=1");
        table2.groupBy("SCHOOL");
        table2.having("COUNT(*) > 1");
        table2.orderBy("ID", false);
        table2.limit(0, 1);
        table2.setSubQuery(true);

        QueryNode subQuery = new QueryNode(table2);
        subQuery.select("C.NAME");

        IColumn column = ASTNodeFactory.getInstance().createColumn().setColumnName("NAME");
        IBooleanFilter filter = ASTNodeFactory.getInstance()
            .createBooleanFilter()
            .setColumn(column)
            .setValue(subQuery)
            .setOperation(OPERATION.EQ);
        table1.query(filter);
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testWhere_表子查询() throws QueryException, SqlParserException {
        String sql = "SELECT NAME FROM (SELECT * FROM TABLE1 A WHERE A.ID=1) B";
        QueryTreeNode qn = query(sql);
        qn.build();

        TableNode table1 = new TableNode("TABLE1");
        table1.subAlias("A").alias("B").query("A.ID=1");
        QueryTreeNode qnExpected = new QueryNode(table1);
        qnExpected.select("NAME");
        qnExpected.build();
        assertEquals(qn, qnExpected);
    }

    @Test
    public void testWhere_字段_表_复杂子查询() throws QueryException, SqlParserException {
        String subSql = "SELECT B.* FROM TABLE2 B WHERE B.ID=1 GROUP BY SCHOOL HAVING COUNT(*) > 1 ORDER BY ID DESC LIMIT 1";
        String sql = "SELECT NAME FROM (SELECT * FROM TABLE1 A WHERE A.ID=1) A WHERE NAME=(SELECT C.NAME FROM ("
                     + subSql + ") C )";
        QueryTreeNode qn = query(sql);
        qn.build();

        System.out.println(qn);
    }

    @Test
    public void testQuery_join子查询_join表() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM (SELECT A.ID,A.NAME FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1) C JOIN TABLE3 D ON C.ID = D.ID";
        QueryTreeNode qn = query(sql);
        qn.build();

        // System.out.println(qn);
        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof JoinNode);
    }

    @Test
    public void testQuery_join子查询() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM (SELECT A.ID,A.NAME FROM TABLE1 A JOIN TABLE2 B ON A.ID=B.ID WHERE A.NAME=1) C WHERE C.ID = 6";
        QueryTreeNode qn = query(sql);
        qn.build();

        // System.out.println(qn);
        // 第一级是QueryNode
        // 第二级是JoinNode
        Assert.assertTrue(qn instanceof QueryNode);
        Assert.assertTrue(((QueryNode) qn).getChild() instanceof JoinNode);
    }

    @Test
    public void testQuery_join_子查询_多级组合() throws SqlParserException, QueryException {
        String joinSql = "SELECT TABLE1.ID,TABLE1.NAME FROM TABLE1 JOIN TABLE2 ON TABLE1.ID=TABLE1.ID WHERE TABLE1.NAME=1";
        String subsql = "SELECT * FROM (" + joinSql + " ) S WHERE S.NAME = 1";
        String sql = "SELECT * FROM (" + subsql + ") B , (" + subsql + ") C WHERE B.NAME = 6 AND B.ID = C.ID";
        QueryTreeNode qn = query(sql);
        qn.build();

        // System.out.println(qn);
        // 第一级是JoinNode
        // 第二级是QuerNode / QueryNode
        // 第三级是JoinNode / JoinNode
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof QueryNode);
        Assert.assertTrue(((JoinNode) qn).getRightNode() instanceof QueryNode);
        Assert.assertTrue(((QueryNode) ((JoinNode) qn).getLeftNode()).getChild() instanceof JoinNode);
        Assert.assertTrue(((QueryNode) ((JoinNode) qn).getRightNode()).getChild() instanceof JoinNode);
    }

    @Test
    public void testQuery_多字段in() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE (ID,NAME) IN ((1,2),(2,3))";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    public void testQuery_join_子查询_in模式() throws SqlParserException, QueryException {
        String sql = "SELECT * FROM TABLE1 WHERE ID IN (SELECT ID FROM TABLE2)";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    // @Test
    public void testQuery_join_子查询_exist模式() throws SqlParserException, QueryException {
        // ExistsPrimary
        String sql = "SELECT * FROM TABLE1 WHERE EXISTS (SELECT ID FROM TABLE2)";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    // @Test
    public void testQuery_join_子查询_all模式() throws SqlParserException, QueryException {
        // SubqueryAllExpression
        String sql = "SELECT * FROM TABLE1 WHERE ID > ALL (SELECT ID FROM TABLE2)";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    // @Test
    public void testQuery_join_子查询_any模式() throws SqlParserException, QueryException {
        // SubqueryAnyExpression
        String sql = "SELECT * FROM TABLE1 WHERE ID > ANY (SELECT ID FROM TABLE2)";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    // @Test
    public void testQuery_不带表() throws SqlParserException, QueryException {
        String sql = "SELECT 1";
        QueryTreeNode qn = query(sql);
        qn.build();
        System.out.println(qn);
    }

    // ==================================================

    private QueryTreeNode query(String sql) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        QueryTreeNode qn = null;
        if (sm.getSqlType() == SqlType.SELECT) {
            qn = sm.getQueryTreeNode(null);
        } else {
            qn = new KVIndexNode(null);
            qn.setSql(sql);
        }
        return qn;
    }

    private QueryTreeNode query(String sql, List args) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        QueryTreeNode qn = null;
        if (sm.getSqlType() == SqlType.SELECT) {
            qn = sm.getQueryTreeNode(convert(args));
        } else {
            qn = new KVIndexNode(null);
            qn.setSql(sql);
        }
        return qn;
    }

    private UpdateNode update(String sql) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        UpdateNode qn = null;
        if (sm.getSqlType() == SqlType.UPDATE) {
            qn = sm.getUpdateNode(null);
        }

        return qn;
    }

    private DeleteNode delete(String sql) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        DeleteNode qn = null;
        if (sm.getSqlType() == SqlType.DELETE) {
            qn = sm.getDeleteNode(null);
        }

        return qn;
    }

    private InsertNode insert(String sql) throws SqlParserException {
        SqlAnalysisResult sm = parser.parse(sql, false);
        InsertNode qn = null;
        if (sm.getSqlType() == SqlType.INSERT) {
            qn = sm.getInsertNode(null);
        }

        return qn;
    }

    private void assertEquals(QueryTreeNode qn, QueryTreeNode qnExpected) {
        logger.debug(qn.toString());
        Assert.assertEquals(qnExpected.toString(), qn.toString());
    }

    private void assertEquals(UpdateNode un, UpdateNode unExpected) {
        logger.debug(un.toString());
        Assert.assertEquals(unExpected.toString(), un.toString());
    }

    private void assertEquals(DeleteNode dn, DeleteNode dnExpected) {
        logger.debug(dn.toString());
        Assert.assertEquals(dnExpected.toString(), dn.toString());
    }

    private void assertEquals(InsertNode in, InsertNode inExpected) {
        logger.debug(in.toString());
        Assert.assertEquals(inExpected.toString(), in.toString());
    }
}
