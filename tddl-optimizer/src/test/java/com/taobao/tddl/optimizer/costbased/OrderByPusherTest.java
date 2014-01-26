package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.MergeNode;
import com.taobao.tddl.optimizer.core.ast.query.QueryNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.plan.query.IJoin.JoinStrategy;
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
    public void test_order条件下推_子表_case2_强制下推() {
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
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_order条件下推_子表_case3_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");
        table1.limit(0, 10);

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
    public void test_order条件下推_子表_case4_下推IDNAME() {
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
    public void test_order条件下推_子表_case5_不下推函数() {
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
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case2_强制下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table1.orderBy("ID");
        table1.orderBy("NAME");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        join.orderBy("A.NAME");
        join.orderBy("A.SCHOOL");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case3_不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        QueryNode query = new QueryNode(table1);
        query.orderBy("ID");
        query.orderBy("NAME");
        query.limit(0, 10);

        table2.alias("B");

        JoinNode join = query.join(table2);
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        join.orderBy("A.NAME");
        join.orderBy("A.SCHOOL");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, query.getOrderBys().size());
        Assert.assertEquals("A.ID", query.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("A.NAME", query.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case4_下推IDNAME() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        join.orderBy("A.ID");
        join.orderBy("A.NAME");
        join.build();

        OrderByPusher.optimize(join);

        Assert.assertEquals(2, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.ID", table1.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(1).getColumn().toString());
    }

    @Test
    public void test_join条件下推_子表_case5_函数不下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        table1.alias("A");
        table2.alias("B");

        JoinNode join = table1.join(table2);
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
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
        join.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
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
        nextJoin.setJoinStrategy(JoinStrategy.INDEX_NEST_LOOP);
        nextJoin.orderBy("B.ID ASC");
        nextJoin.orderBy("B.NAME DESC");
        nextJoin.build();

        OrderByPusher.optimize(nextJoin);

        // 最左节点会有两个order by push, ID和NAME
        Assert.assertEquals(2, table1.getOrderBys().size());
    }

    @Test
    public void test_orderby_SortMerge下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2).addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.setOuterJoin().setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
        join.select("TABLE1.ID AS ID , TABLE1.NAME AS NAME , TABLE1.SCHOOL AS SCHOOL");
        join.groupBy("NAME").groupBy("SCHOOL").groupBy("ID"); // group by顺序可调整
        join.orderBy("ID", false);
        join.build();

        OrderByPusher.optimize(join);
        // 推出来的结果：
        // 1. 按照join列先推，ID , NAME的排序
        // 2. 按照order by + group by的推成功，最后排序结果为ID,NAME,SCHOOL
        Assert.assertEquals("TABLE1.ID", join.getLeftNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getLeftNode().getOrderBys().get(0).getDirection()); // 逆序
        Assert.assertEquals("TABLE1.NAME", join.getLeftNode().getOrderBys().get(1).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL", join.getLeftNode().getOrderBys().get(2).getColumn().toString());

        Assert.assertEquals("TABLE2.ID", join.getRightNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getRightNode().getOrderBys().get(0).getDirection()); // 逆序
        Assert.assertEquals("TABLE2.NAME", join.getRightNode().getOrderBys().get(1).getColumn().toString());

        Assert.assertEquals("TABLE1.ID as ID", join.getGroupBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME as NAME", join.getGroupBys().get(1).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL as SCHOOL", join.getGroupBys().get(2).getColumn().toString());

        Assert.assertEquals("TABLE1.ID as ID", join.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getOrderBys().get(0).getDirection()); // 逆序
    }

    @Test
    public void test_orderby_SortMerge下推_只推group() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2).addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.setOuterJoin().setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
        join.select("TABLE1.ID AS ID , TABLE1.NAME AS NAME , TABLE1.SCHOOL AS SCHOOL");
        join.groupBy("NAME").groupBy("SCHOOL").groupBy("ID"); // group by顺序可调整
        join.orderBy("SCHOOL", false);
        join.build();

        OrderByPusher.optimize(join);
        // 推出来的结果：
        // 1. 按照join列先推，ID , NAME的排序
        // 2. 按照order by + group by的会推不成功，因为是按照school字段顺序
        // 3. 按照group by单独推会成功
        Assert.assertEquals("TABLE1.ID", join.getLeftNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME", join.getLeftNode().getOrderBys().get(1).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL", join.getLeftNode().getOrderBys().get(2).getColumn().toString());
        Assert.assertEquals(false, join.getLeftNode().getOrderBys().get(2).getDirection()); // 逆序

        Assert.assertEquals("TABLE2.ID", join.getRightNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE2.NAME", join.getRightNode().getOrderBys().get(1).getColumn().toString());

        Assert.assertEquals("TABLE1.ID as ID", join.getGroupBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.NAME as NAME", join.getGroupBys().get(1).getColumn().toString());
        Assert.assertEquals("TABLE1.SCHOOL as SCHOOL", join.getGroupBys().get(2).getColumn().toString());

        Assert.assertEquals("TABLE1.SCHOOL as SCHOOL", join.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getOrderBys().get(0).getDirection()); // 逆序
    }

    @Test
    public void test_orderby_SortMerge下推_调整join顺序() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        JoinNode join = table1.join(table2).addJoinKeys("ID", "ID").addJoinKeys("NAME", "NAME");
        join.setOuterJoin().setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
        join.select("TABLE1.ID AS ID , TABLE1.NAME AS NAME , TABLE1.SCHOOL AS SCHOOL");
        join.groupBy("NAME").groupBy("SCHOOL"); // group by顺序可调整
        join.orderBy("NAME", false);
        join.build();

        OrderByPusher.optimize(join);
        // 推出来的结果：
        // 1. 按照join列先推，ID , NAME的排序
        // 2. 按照order by + group by的会推不成功，因为是按照NAME+SCHOOL字段顺序
        Assert.assertEquals("TABLE1.NAME", join.getLeftNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE1.ID", join.getLeftNode().getOrderBys().get(1).getColumn().toString());
        Assert.assertEquals(false, join.getLeftNode().getOrderBys().get(0).getDirection()); // 逆序

        Assert.assertEquals("TABLE2.NAME", join.getRightNode().getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals("TABLE2.ID", join.getRightNode().getOrderBys().get(1).getColumn().toString());
        Assert.assertEquals(false, join.getRightNode().getOrderBys().get(0).getDirection()); // 逆序

        Assert.assertEquals("TABLE1.NAME as NAME", join.getGroupBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getGroupBys().get(0).getDirection()); // 逆序
        Assert.assertEquals("TABLE1.SCHOOL as SCHOOL", join.getGroupBys().get(1).getColumn().toString());

        Assert.assertEquals("TABLE1.NAME as NAME", join.getOrderBys().get(0).getColumn().toString());
        Assert.assertEquals(false, join.getOrderBys().get(0).getDirection()); // 逆序
    }

    @Test
    public void test_orderby_SortMerge下推_多级结构下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        // 如果底层join的顺序不是ID,NAME的顺序，暂时没法推，要递归做最优，算法太复杂，先简单只考虑一层
        JoinNode join = table1.join(table2).addJoinKeys("ID", "ID");
        join.setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
        join.alias("S").select("TABLE1.ID AS AID , TABLE1.NAME AS ANAME , TABLE1.SCHOOL AS ASCHOOL");
        join.build();

        QueryNode queryA = new QueryNode(join);
        queryA.alias("B");
        queryA.select("S.AID AS BID,S.ANAME AS BNAME,S.ASCHOOL AS BSCHOOL");
        queryA.build();

        QueryNode queryB = queryA.deepCopy();
        queryB.alias("C");
        queryB.select("S.AID AS CID,S.ANAME AS CNAME,S.ASCHOOL AS CSCHOOL");
        queryB.build();

        JoinNode nextJoin = queryA.join(queryB).addJoinKeys("BID", "CID").addJoinKeys("BNAME", "CNAME");
        nextJoin.setJoinStrategy(JoinStrategy.SORT_MERGE_JOIN);
        nextJoin.select("C.CID AS ID , C.CNAME AS NAME , C.CSCHOOL AS SCHOOL");
        // group by顺序可调整
        nextJoin.groupBy("NAME").groupBy("SCHOOL").groupBy("ID");
        nextJoin.orderBy("SCHOOL", false);
        nextJoin.build();

        OrderByPusher.optimize(nextJoin);
        // 推导结果有点深，就不枚举了

        // 左子树，ID , NAME
        Assert.assertEquals(2, ((TableNode) queryA.getChild().getChildren().get(0)).getOrderBys().size());
        // 只是id join列
        Assert.assertEquals(1, ((TableNode) queryA.getChild().getChildren().get(1)).getOrderBys().size());
        // ID , NAME
        Assert.assertEquals(2, queryA.getOrderBys().size());

        // 右子树，ID , NAME , SCHOOL
        Assert.assertEquals(3, ((TableNode) queryB.getChild().getChildren().get(0)).getOrderBys().size());
        // 只是id join列
        Assert.assertEquals(1, ((TableNode) queryB.getChild().getChildren().get(1)).getOrderBys().size());
        // ID , NAME , SCHOOL
        Assert.assertEquals(3, queryB.getOrderBys().size());

        // ID
        Assert.assertEquals(1, nextJoin.getOrderBys().size());
        // ID , NAME , SCHOOL
        Assert.assertEquals(3, nextJoin.getGroupBys().size());
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

    @Test
    public void test_join的distinct下推() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");

        IColumn id = ASTNodeFactory.getInstance().createColumn();
        id.setColumnName("ID");
        id.setTableName("TABLE1");
        id.setDistinct(true);

        IColumn name = ASTNodeFactory.getInstance().createColumn();
        name.setColumnName("NAME");
        name.setTableName("TABLE1");
        name.setDistinct(true);

        IColumn school = ASTNodeFactory.getInstance().createColumn();
        school.setColumnName("SCHOOL");
        school.setTableName("TABLE1");
        school.setDistinct(true);

        JoinNode join = table1.join(table2);
        join.select(id, name, school);
        join.orderBy("NAME");
        join.build();

        OrderByPusher.optimize(join);
        Assert.assertEquals(3, table1.getOrderBys().size());
        Assert.assertEquals("TABLE1.NAME", table1.getOrderBys().get(0).getColumn().toString());
    }
}
