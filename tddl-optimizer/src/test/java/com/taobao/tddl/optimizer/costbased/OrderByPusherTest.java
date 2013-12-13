package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.ast.query.strategy.IndexNestedLoopJoin;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.costbased.pusher.OrderByPusher;

public class OrderByPusherTest extends BaseOptimizerTest {

    @Test
    public void test_order条件下推_子表_case1_下推NAME() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");
        table1.orderBy("ID");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.ID");
        query.orderBy("A.NAME");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case2_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.NAME");
        query.orderBy("A.SCHOOL");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case3_下推IDNAME() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");

        QueryNode query = new QueryNode(table1);
        query.orderBy("A.ID");
        query.orderBy("A.NAME");
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(2, query.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case4_不下推函数() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");

        QueryNode query = new QueryNode(table1);
        query.select("ID AS CID, (NAME+SCHOOL) AS NAME");
        query.orderBy("A.CID ");
        query.orderBy("A.NAME"); // 这里的name为select中的函数
        query.build();

        OrderByPusher.optimize(query);

        Assert.assertEquals(0, table1.getOrderBys().size());
    }

    @Test
    public void test_join条件下推_子表_case1_下推NAME() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table1.orderBy("ID");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case2_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.NAME");
        join.orderBy("A.SCHOOL");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case3_下推IDNAME() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case4_函数不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.select("A.ID AS CID, (A.NAME + A.SCHOOL) AS NAME");
        join.orderBy("CID ");
        join.orderBy("NAME"); // 这里的name为select中的函数
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(0, table1.getOrderBys().size());
    }

    @Test
    public void test_orderby多级结构下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(new IndexNestedLoopJoin());
        join.alias("S").select("TABLE1.ID AS ID , TABLE1.NAME AS NAME , TABLE2.SCHOOL AS SCHOOL");
        join.build();

        QueryNode queryA = new QueryNode(join);
        queryA.alias("B");
        queryA.select("S.ID AS ID,S.NAME AS NAME");
        queryA.build();

        QueryNode queryB = queryA.deepCopy();
        queryB.alias("C");
        queryB.select("S.SCHOOL AS SCHOOL");
        queryB.build();

        JoinNode nextJoin = queryA.join(queryB);
        nextJoin.setJoinStrategy(new IndexNestedLoopJoin());
        nextJoin.orderBy("B.ID ASC");
        nextJoin.orderBy("B.NAME DESC");
        nextJoin.build();

        OrderByPusher.optimize(nextJoin);

        // 最左节点会有两个order by push, ID和NAME
        Assert.assertEquals(2, table1.getOrderBys().size());
    }

    @Test
    public void test_merge的distinct下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        IColumn id = ASTNodeFactory.getInstance().createColumn();
        id.setColumnName("ID");
        id.setDistinct(true);

        IColumn name = ASTNodeFactory.getInstance().createColumn();
        name.setColumnName("NAME");
        name.setDistinct(true);

        IColumn school = ASTNodeFactory.getInstance().createColumn();
        school.setColumnName("SCHOOL");
        school.setDistinct(true);

        table1.groupBy("NAME");
        table1.orderBy("ID");

        MergeNode merge = table1.merge(table2);
        merge.select(id, name, school);
        merge.build();

        OrderByPusher.optimize(merge);
        Assert.assertEquals(3, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(0).getColumn().toString());
    }
}
