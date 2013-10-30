package com.taobao.tddl.rule.exceptions;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class TddlRuleException extends TddlRuntimeException {

    private static final long serialVersionUID = 4403809817705460724L;

    public TddlRuleException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public TddlRuleException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public TddlRuleException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public TddlRuleException(String errorCode){
        super(errorCode);
    }

    public TddlRuleException(Throwable cause){
        super(cause);
    }

}
