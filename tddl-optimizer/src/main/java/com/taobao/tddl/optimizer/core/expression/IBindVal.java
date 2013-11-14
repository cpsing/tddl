package com.taobao.tddl.optimizer.core.expression;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.PlanVisitor;

/**
 * 绑定变量
 */
public interface IBindVal extends Comparable, CanVisit {

    public Comparable assignment(Map<Integer, ParameterContext> parameterSettings);

    void accept(PlanVisitor visitor);
}
