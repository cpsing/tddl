package com.taobao.tddl.matrix.config;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.config.impl.holder.AbstractConfigDataHolder;
import com.taobao.tddl.config.impl.holder.ConfigHolderFactory;
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

/**
 * 依赖的组件
 * 
 * @since 5.0.0
 */
public class MatrixConfigHolder extends AbstractConfigDataHolder {

    private static final String GROUP_CONFIG_HOLDER_NAME = "com.taobao.tddl.group.config.GroupConfigHolder";
    private String              appName;
    private String              ruleFilePath;
    private boolean             dynamicRule;
    private String              schemaFilePath;
    private String              topologyFilePath;
    private String              unitName;
    private OptimizerRule       optimizerRule;
    private TopologyHandler     topologyHandler;
    private TopologyExecutor    topologyExecutor;
    private SchemaManager       schemaManager;
    private IndexManager        indexManger;
    private Optimizer           optimizer;
    private OptimizerContext    optimizerContext;
    private ExecutorContext     executorContext;
    private StatManager         statManager;
    private Matrix              matrix;
    private Map<String, Object> connectionProperties;
    private boolean             createGroupExecutor      = true;

    @Override
    public void doInit() throws TddlException {
        loadDelegateExtension();

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
        executorContext.setTopologyExecutor(topologyExecutor);

        oc.setIndexManager(this.indexManger);
        oc.setMatrix(topologyHandler.getMatrix());
        oc.setSchemaManager(schemaManager);
        oc.setRule(optimizerRule);
        oc.setOptimizer(this.optimizer);
        oc.setStatManager(this.statManager);

        matrix = topologyHandler.getMatrix();
        // 添加matrix参数
        addDatas(matrix.getProperties());
        initSonHolder();
        // 将自己做为config holder
        ConfigHolderFactory.addConfigDataHolder(appName, this);
        if (createGroupExecutor) {
            initGroups();
        }
    }

    @Override
    protected void doDestory() throws TddlException {
        schemaManager.destory();
        optimizerRule.destory();
        optimizer.destory();
        statManager.destory();
        topologyHandler.destory();
        topologyExecutor.destory();
    }

    public void topologyInit() throws TddlException {
        topologyHandler = new TopologyHandler(appName, unitName, topologyFilePath);
        topologyHandler.init();

        topologyExecutor = new TopologyExecutor();
        topologyExecutor.init();
    }

    public void ruleInit() throws TddlException {
        TddlRule rule = new TddlRule();
        rule.setAppName(appName);
        rule.setAppRuleFile(ruleFilePath);
        rule.init();

        optimizerRule = new OptimizerRule(rule);
        optimizerRule.init();
    }

    public void schemaInit() throws TddlException {
        RuleSchemaManager ruleSchemaManager = new RuleSchemaManager(optimizerRule,
            topologyHandler.getMatrix(),
            GeneralUtil.getExtraCmdLong(this.connectionProperties,
                ExtraCmd.TABLE_META_CACHE_EXPIRE_TIME,
                TddlConstants.DEFAULT_TABLE_META_EXPIRE_TIME));
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
        optimizer.setExpireTime(GeneralUtil.getExtraCmdLong(this.connectionProperties,
            ExtraCmd.OPTIMIZER_CACHE_EXPIRE_TIME,
            TddlConstants.DEFAULT_OPTIMIZER_EXPIRE_TIME));
        optimizer.init();

        this.optimizer = optimizer;

        // RuleStatManager statManager = new RuleStatManager(optimizerRule,
        // topologyHandler.getMatrix());
        LocalStatManager statManager = new LocalStatManager();
        statManager.init();

        this.statManager = statManager;
    }

    protected void initSonHolder() throws TddlException {
        Class sonHolderClass = null;
        try {
            sonHolderClass = Class.forName(GROUP_CONFIG_HOLDER_NAME);
        } catch (ClassNotFoundException e1) {
            // ignore , 可能不需要使用group层
            return;
        }

        try {
            Constructor constructor = sonHolderClass.getConstructor(String.class, List.class, String.class);
            sonConfigDataHolder = (AbstractConfigDataHolder) constructor.newInstance(this.appName,
                matrix.getGroups(),
                this.unitName);
            sonConfigDataHolder.init();
            delegateDataHolder.setSonConfigDataHolder(sonConfigDataHolder);// 传递给deletegate，由它进行son传递
        } catch (Exception e) {
            throw new TddlException(e);
        }
    }

    protected void initGroups() throws TddlException {
        for (Group group : matrix.getGroups()) {
            topologyHandler.createOne(group);
        }
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

    public Map<String, Object> getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Map<String, Object> connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public boolean isDynamicRule() {
        return dynamicRule;
    }

    public void setDynamicRule(boolean dynamicRule) {
        this.dynamicRule = dynamicRule;
    }

}
