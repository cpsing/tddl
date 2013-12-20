package com.taobao.tddl.optimizer.costbased;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;

public class SubQueryPreProcessorTest extends BaseOptimizerTest {

    @Test
    public void test_等于子查询() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID = (SELECT ID FROM TABLE2)");
        table1.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(table1);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("1", qn.getWhereFilter().toString());
    }

    @Test
    public void test_In子查询() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID IN (SELECT ID FROM TABLE2)");
        table1.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(table1);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("1", qn.getWhereFilter().toString());
    }

    @Test
    public void test_NotIn子查询() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID NOT IN (SELECT ID FROM TABLE2)");
        table1.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(table1);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals("TABLE2.ID IS NULL", qn.getWhereFilter().toString());
    }

    @Test
    public void test_等于子查询_存在OR查询_报错() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID = (SELECT ID FROM TABLE2 WHERE NAME = 'HELLO') OR NAME = 3");
        table1.build();

        try {
            SubQueryPreProcessor.optimize(table1);
            Assert.fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void test_等于子查询_存在关联条件_报错() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID = (SELECT ID FROM TABLE2 WHERE TABLE1.NAME = TABLE2.NAME)");
        try {
            table1.build(); // 编译出错，不支持
            Assert.fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void test_等于子查询_存在多条件() {
        TableNode table1 = new TableNode("TABLE1");
        table1.query("ID = (SELECT ID FROM TABLE2 WHERE NAME = 'HELLO' AND SCHOOL = 'HELLO' ) AND NAME = 3 AND SCHOOL IN ('A','B')");
        table1.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(table1);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertEquals(null, table1.getWhereFilter());
    }

    @Test
    public void test_等于子查询_外部是个join节点() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2, "ID", "ID");
        join.query("TABLE1.ID = (SELECT ID FROM TABLE3 WHERE NAME = 'HELLO' AND SCHOOL = 'HELLO' ) AND TABLE1.NAME = 3 AND TABLE1.SCHOOL IN ('A','B')");
        join.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(join);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof JoinNode);
        Assert.assertEquals(null, join.getWhereFilter());
    }

    @Test
    public void test_等于IN组合子查询_复杂条件_外部是个join节点() {
        TableNode table1 = new TableNode("TABLE1");
        TableNode table2 = new TableNode("TABLE2");
        JoinNode join = table1.join(table2, "ID", "ID");
        join.query("TABLE1.ID = (SELECT ID FROM TABLE3 WHERE NAME = 'HELLO' AND SCHOOL = 'HELLO' ) AND TABLE1.NAME = 3 AND TABLE1.SCHOOL IN (SELECT SCHOOL FROM TABLE4)");
        join.build();

        QueryTreeNode qn = SubQueryPreProcessor.optimize(join);
        Assert.assertTrue(qn instanceof JoinNode);
        Assert.assertTrue(((JoinNode) qn).getLeftNode() instanceof JoinNode);
        Assert.assertTrue(((JoinNode) ((JoinNode) qn).getLeftNode()).getLeftNode() instanceof JoinNode);
        Assert.assertEquals(null, join.getWhereFilter());
    }
}
