package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.exceptions.QueryException;

public interface RelationQueryOptimizer {

    ASTNode optimize(ASTNode node, Map<Integer, ParameterContext> parameterSettings, Map<String, Comparable> extraCmd)
                                                                                                                      throws QueryException;
}
