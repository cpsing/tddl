package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * <pre>
 * 对执行计划树的优化，包含以下几个步骤： 
 * s8.为执行计划的每个节点选择执行的GroupNode
 *      这一步是根据TDDL的规则进行分库 在Join，Merge的执行节点选择上，遵循的原则是尽量减少网络传输 
 *  
 * s9.调整分库后的Join节点
 *      由于分库后，一个Query节点可能会变成一个Merge节点，需要对包含这样子节点的Join节点进行调整，详细见splitJoinAfterChooseDataNode的注释
 * </pre>
 * 
 * @since 5.0.0
 */
public interface QueryPlanOptimizer {

    IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                               Map<String, Object> extraCmd) throws QueryException;
}
