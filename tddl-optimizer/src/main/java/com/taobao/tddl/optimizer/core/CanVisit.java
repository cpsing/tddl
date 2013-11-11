package com.taobao.tddl.optimizer.core;

public interface CanVisit {

    void accept(PlanVisitor visitor);
}
