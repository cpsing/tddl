package com.taobao.tddl.matrix.config;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.TopologyExecutor;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.TopologyHandler;
import com.taobao.tddl.optimizer.Optimizer;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.config.table.StaticSchemaManager;
import com.taobao.tddl.optimizer.costbased.CostBasedOptimizer;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.LocalStatManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.StatManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.rule.RuleIndexManager;
import com.taobao.tddl.optimizer.rule.RuleSchemaManager;
import com.taobao.tddl.rule.TddlRule;

public class ConfigHolder extends AbstractLifecycle {

    final static Logger      logger = LoggerFactory.getLogger(ConfigHolder.class);
    OptimizerRule            optimizerRule;
    private String           appName;
    private String           ruleFilePath;
    private String           schemaFilePath;
    private String           topologyFilePath;
    private String           unitName;
    private TopologyHandler  topologyHandler;
    private SchemaManager    schemaManager;
    private IndexManager     indexManger;
    private Optimizer        optimizer;
    private OptimizerContext optimizerContext;
    private ExecutorContext  executorContext;
    private StatManager      statManager;

    @Override
    public void doInit() throws TddlException {

        ExecutorContext executorContext = new ExecutorContext();
        this.executorContext = executorContext;
        ExecutorContext.setContext(executorContext);

        OptimizerContext oc = new OptimizerContext();
        this.optimizerContext = oc;
        OptimizerContext.setContext(oc);

        topologyInit();
        ruleInit();
        schemaInit();
        optimizerInit();

        executorContext.setTopologyHandler(topologyHandler);
        executorContext.setTopologyExecutor(new TopologyExecutor());

        oc.setIndexManager(this.indexManger);
        oc.setMatrix(topologyHandler.getMatrix());
        oc.setSchemaManager(schemaManager);
        oc.setRule(optimizerRule);
        oc.setOptimizer(this.optimizer);
        oc.setStatManager(this.statManager);
    }

    public void topologyInit() throws TddlException {

        topologyHandler = new TopologyHandler(appName, unitName, topologyFilePath);
        topologyHandler.init();
    }

    public void ruleInit() throws TddlException {
        TddlRule rule = new TddlRule();
        rule.setAppName(appName);
        rule.setAppRuleFile(ruleFilePath);
        rule.init();

        optimizerRule = new OptimizerRule(rule);
    }

    public void schemaInit() throws TddlException {
        RuleSchemaManager ruleSchemaManager = new RuleSchemaManager(optimizerRule, topologyHandler.getMatrix());
        StaticSchemaManager staticSchemaManager = new StaticSchemaManager(schemaFilePath, appName, unitName);

        ruleSchemaManager.setLocal(staticSchemaManager);

        this.schemaManager = ruleSchemaManager;

        schemaManager.init();

        IndexManager indexManager = new RuleIndexManager(ruleSchemaManager);
        indexManager.init();

        this.indexManger = indexManager;
    }

    public void optimizerInit() throws TddlException {

        CostBasedOptimizer optimizer = new CostBasedOptimizer();
        optimizer.init();

        this.optimizer = optimizer;

        // RuleStatManager statManager = new RuleStatManager(optimizerRule,
        // topologyHandler.getMatrix());
        LocalStatManager statManager = new LocalStatManager();
        statManager.init();

        this.statManager = statManager;

    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getRuleFilePath() {
        return ruleFilePath;
    }

    public void setRuleFilePath(String ruleFilePath) {
        this.ruleFilePath = ruleFilePath;
    }

    public String getSchemaFilePath() {
        return schemaFilePath;
    }

    public void setSchemaFilePath(String schemaFilePath) {
        this.schemaFilePath = schemaFilePath;
    }

    public String getTopologyFilePath() {
        return topologyFilePath;
    }

    public void setTopologyFilePath(String topologyFilePath) {
        this.topologyFilePath = topologyFilePath;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public ExecutorContext getExecutorContext() {
        return this.executorContext;
    }

    public OptimizerContext getOptimizerContext() {
        return this.optimizerContext;
    }

}
