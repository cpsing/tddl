package com.taobao.tddl.optimizer;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 优化器执行接口
 * 
 * @since 5.1.0
 */
public interface Optimizer {

    /**
     * @param node 查询树
     * @param parameterSettings 参数
     * @param extraCmd 额外的优化参数，控制优化行为
     * @return
     * @throws QueryException
     */
    IDataNodeExecutor optimizeAndAssignment(ASTNode node, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Comparable> extraCmd) throws QueryException;

    /**
     * @param node 查询树
     * @param parameterSettings 参数
     * @param extraCmd 额外的优化参数，控制优化行为
     * @param sql 对应生成优化树的原始sql
     * @param cached 是否缓存
     * @return
     * @throws QueryException
     */
    IDataNodeExecutor optimizeAndAssignment(ASTNode node, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Comparable> extraCmd, String sql, boolean cached)
                                                                                                         throws QueryException;
}
