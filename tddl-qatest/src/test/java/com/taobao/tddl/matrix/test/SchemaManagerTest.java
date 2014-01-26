package com.taobao.tddl.matrix.test;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.TopologyHandler;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.rule.RuleSchemaManager;
import com.taobao.tddl.rule.TddlRule;

public class SchemaManagerTest {

    @Test
    public void initTestStaticSchemaManager() throws TddlException {
        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);

        StaticSchemaManager s = new StaticSchemaManager(null, "andor_show", null);
        s.init();

        Assert.assertTrue(s.getTable("BMW_USERS") != null);
        Assert.assertEquals(8, s.getAllTables().size());
    }

    @Test
    public void initTestStaticSchemaManagerWithSchemaFile() throws TddlException {
        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);
        StaticSchemaManager s = new StaticSchemaManager("test_schema.xml", "andor_show", null);
        s.init();

        Assert.assertTrue(s.getTable("BMW_USERS") != null);
        Assert.assertEquals(9, s.getAllTables().size());
    }

    @Test
    public void initTestRuleSchemaManager() throws TddlException {
        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);
        TopologyHandler topology = new TopologyHandler("andor_show", null, "test_matrix_without_group_config.xml");
        executorContext.setTopologyHandler(topology);

        topology.init();
        TddlRule rule = new TddlRule();
        rule.setAppName("andor_show");
        rule.init();

        OptimizerRule optimizerRule = new OptimizerRule(rule);
        RuleSchemaManager s = new RuleSchemaManager(optimizerRule, topology.getMatrix());
        s.init();
        Assert.assertTrue(s.getTable("BMW_USERS") != null);
    }
}
