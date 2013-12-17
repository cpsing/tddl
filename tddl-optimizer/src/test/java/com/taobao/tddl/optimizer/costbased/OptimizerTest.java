package com.taobao.tddl.optimizer.costbased;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 整个优化器的集成测试，主要是一些query
 */
public class OptimizerTest extends BaseOptimizerTest {

    private static Map<String, Comparable> extraCmd = new HashMap<String, Comparable>();

    @BeforeClass
    public static void setUp() {
        extraCmd.put(ExtraCmd.OptimizerExtraCmd.ChooseIndex, true);
        extraCmd.put(ExtraCmd.OptimizerExtraCmd.ChooseJoin, false);
        extraCmd.put(ExtraCmd.OptimizerExtraCmd.ChooseIndexMerge, false);
        extraCmd.put(ExtraCmd.OptimizerExtraCmd.JoinMergeJoinJudgeByRule, true);
    }

    @Test
    public void test_单表查询_无条件() {
        TableNode table = new TableNode("TABLE1");
        QueryTreeNode qn = table;
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(qn, null, null);

        Assert.assertTrue(qc instanceof IMerge);
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals(null, query.getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 直接查询KV ID->data
    // keyFilter为ID=1
    @Test
    public void test_单表查询_主键条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID=1 AND ID<40");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, null);
        Assert.assertTrue(qc instanceof IQuery);
        Assert.assertEquals("TABLE1.ID = 1", ((IQuery) qc).getKeyFilter().toString());
        Assert.assertEquals(null, ((IQuery) qc).getValueFilter());
    }

    // 单表主键查询
    // ID为主键，同时在ID上存在索引
    // 因为!=不能使用主键索引
    // valueFilter为ID!=1
    @Test
    public void test_单表查询_主键条件_不等于只能是valueFilter() {
        TableNode table = new TableNode("TABLE1");
        table.query("ID != 1");
        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, null);

        Assert.assertTrue(qc instanceof IMerge);
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IQuery);
        IQuery query = (IQuery) dne;
        Assert.assertEquals(null, query.getKeyFilter());
        Assert.assertEquals("TABLE1.ID != 1", query.getValueFilter().toString());
    }

    // 单表非主键索引查询
    // NAME上存在索引
    // 会生成一个Join节点
    // 左边通过NAME索引找到满足条件的PK，keyFilter应该为NAME=1
    // 与pk->data Join
    // Join类型为IndexNestLoop
    @Test
    public void test_单表查询_value条件() {
        TableNode table = new TableNode("TABLE1");
        table.query("NAME = 1");

        IQueryTree qc = (IQueryTree) optimizer.optimizeAndAssignment(table, null, null);
        Assert.assertTrue(qc instanceof IMerge);
        IDataNodeExecutor dne = ((IMerge) qc).getSubNode().get(0);
        Assert.assertTrue(dne instanceof IJoin);
        IJoin join = (IJoin) dne;
        IQuery left = (IQuery) join.getLeftNode();
        Assert.assertEquals("TABLE1._NAME.NAME = 1", left.getKeyFilter().toString());
    }

}
