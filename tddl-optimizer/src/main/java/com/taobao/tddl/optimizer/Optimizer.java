package com.taobao.tddl.optimizer;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;

/**
 * 优化器执行接口
 * 
 * @since 5.0.0
 */
public interface Optimizer extends Lifecycle {

    /**
     * 根据自定义查询树生成执行计划，不做缓存处理
     * 
     * @param node 查询树
     * @param parameterSettings 参数
     * @param extraCmd 额外的优化参数，控制优化行为
     * @return
     * @throws QueryException
     */
    IDataNodeExecutor optimizeAndAssignment(ASTNode node, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Object> extraCmd) throws QueryException;

    /**
     * 根据sql生成执行计划，根据cached参数判断是否进行缓存处理
     * 
     * @param sql 原始sql
     * @param parameterSettings 参数
     * @param extraCmd 额外的优化参数，控制优化行为
     * @param cached 是否缓存
     * @return
     * @throws QueryException
     */
    IDataNodeExecutor optimizeAndAssignment(String sql, Map<Integer, ParameterContext> parameterSettings,
                                            Map<String, Object> extraCmd, boolean cached) throws SqlParserException,
                                                                                         QueryException;
}
