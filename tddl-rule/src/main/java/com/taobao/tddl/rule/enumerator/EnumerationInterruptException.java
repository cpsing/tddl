package com.taobao.tddl.rule.enumerator;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.rule.model.sqljep.Comparative;

public class EnumerationInterruptException extends TddlRuntimeException {

    private static final long     serialVersionUID = 1L;
    private transient Comparative comparative;

    public EnumerationInterruptException(Comparative comparative){
        super(comparative.toString());
        this.comparative = comparative;
    }

    public Comparative getComparative() {
        return comparative;
    }

    public void setComparative(Comparative comparative) {
        this.comparative = comparative;
    }

}
