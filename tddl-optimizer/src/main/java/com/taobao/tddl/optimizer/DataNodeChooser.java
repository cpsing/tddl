package com.taobao.tddl.optimizer;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 基于Rule规则计算目标的执行节点
 * 
 * @since 5.1.0
 */
public interface DataNodeChooser {

    ASTNode shard(ASTNode dne, Map<Integer, ParameterContext> parameterSettings, Map<String, Comparable> extraCmd)
                                                                                                                  throws QueryException;

    ASTNode shard(KVIndexNode dne, IFilter filter, Map<Integer, ParameterContext> parameterSettings,
                  Map<String, Comparable> extraCmd) throws QueryException;
}
