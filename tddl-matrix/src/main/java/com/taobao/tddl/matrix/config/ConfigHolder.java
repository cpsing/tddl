package com.taobao.tddl.matrix.config;

import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.TopologyHandler;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.optimizer.rule.RuleSchemaManager;
import com.taobao.tddl.rule.TddlRule;

public class ConfigHolder extends AbstractLifecycle {

    final static Logger     logger = LoggerFactory.getLogger(ConfigHolder.class);
    OptimizerRule           optimizerRule;
    private String          appName;
    private String          ruleFilePath;
    private String          schemaFilePath;
    private String          topologyFilePath;
    private String          unitName;
    private TopologyHandler topologyHandler;
    private SchemaManager   schemaManager;

    @Override
    public void doInit() {
        topologyInit();
        ruleInit();
        schemaInit();
    }

    public void topologyInit() {

        topologyHandler = new TopologyHandler(appName, unitName, topologyFilePath);
        topologyHandler.init();
    }

    public void ruleInit() {
        TddlRule rule = new TddlRule();
        rule.setAppName(appName);
        rule.setAppRuleFile(ruleFilePath);
        rule.init();

        optimizerRule = new OptimizerRule(rule);
    }

    public void schemaInit() {
        schemaManager = new RuleSchemaManager(optimizerRule, topologyHandler.getMatrix());

        schemaManager.init();
    }

    public void optimizerInit() {

    }
}
