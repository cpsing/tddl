package com.taobao.tddl.optimizer.exceptions;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class OptimizerException extends TddlRuntimeException {

    private static final long serialVersionUID = 4520487604630799374L;

    public OptimizerException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public OptimizerException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public OptimizerException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public OptimizerException(String errorCode){
        super(errorCode);
    }

    public OptimizerException(Throwable cause){
        super(cause);
    }

}
