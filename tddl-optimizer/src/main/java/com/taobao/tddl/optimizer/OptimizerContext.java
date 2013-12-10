package com.taobao.tddl.optimizer;

import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.utils.thread.ThreadLocalMap;
import com.taobao.tddl.optimizer.config.table.IndexManager;
import com.taobao.tddl.optimizer.config.table.SchemaManager;
import com.taobao.tddl.optimizer.costbased.esitimater.stat.StatManager;
import com.taobao.tddl.optimizer.rule.OptimizerRule;

/**
 * 优化器上下文，主要解决一些共享上下文对象，因为不考虑spring进行IOC控制，所以一些对象/工具之间的依赖就很蛋疼，就搞了这么一个上下文
 * 
 * <pre>
 * 考虑基于ThreadLocal进行上下文传递，几个原因：
 * 1. 减少context传递，ast/expression/plan基本都是无状态的，不希望某个特定代码需要依赖context，而导致整个链路对象都需要传递context上下文
 * 2. context上下文中的对象，本身为支持线程安全，每个tddl客户端实例只有一份，一个jvm实例允许多个tddl客户端实例，所以不能搞成static对象
 * </pre>
 * 
 * @author jianghang 2013-11-12 下午3:07:19
 */
public class OptimizerContext {

    private static final String OPTIMIZER_CONTEXT_KEY = "_optimizer_context_";
    // 配置信息
    private Matrix              matrix;
    private SchemaManager       schemaManager;
    private IndexManager        indexManager;
    private OptimizerRule       rule;
    private Optimizer           optimizer;
    private StatManager         statManager;

    public static OptimizerContext getContext() {
        return (OptimizerContext) ThreadLocalMap.get(OPTIMIZER_CONTEXT_KEY);
    }

    public static void setContext(OptimizerContext context) {
        ThreadLocalMap.put(OPTIMIZER_CONTEXT_KEY, context);
    }

    public Matrix getMatrix() {
        return matrix;
    }

    public void setMatrix(Matrix matrix) {
        this.matrix = matrix;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }

    public OptimizerRule getRule() {
        return rule;
    }

    public void setRule(OptimizerRule rule) {
        this.rule = rule;
    }

    public Optimizer getOptimizer() {
        return optimizer;
    }

    public void setOptimizer(Optimizer optimizer) {
        this.optimizer = optimizer;
    }

    public StatManager getStatManager() {
        return statManager;
    }

    public void setStatManager(StatManager statManager) {
        this.statManager = statManager;
    }

}
