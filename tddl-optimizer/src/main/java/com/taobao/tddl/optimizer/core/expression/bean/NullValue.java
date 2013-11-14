package com.taobao.tddl.optimizer.core.expression.bean;

import com.taobao.tddl.optimizer.core.CanVisit;
import com.taobao.tddl.optimizer.core.PlanVisitor;

public class NullValue implements Comparable, CanVisit {

    private static NullValue instance = new NullValue();
    private String           str      = "null";

    public static NullValue getNullValue() {
        return instance;
    }

    private NullValue(){
    }

    public int compareTo(Object o) {
        if (o == this) {
            return 0;
        }
        if (o instanceof NullValue) {
            return 0;
        }
        return -1;

    }

    public String toString() {
        return str;
    }

    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

}
