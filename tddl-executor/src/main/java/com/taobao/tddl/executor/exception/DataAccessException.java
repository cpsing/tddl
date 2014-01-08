package com.taobao.tddl.executor.exception;

import com.taobao.tddl.common.exception.TddlException;

public class DataAccessException extends TddlException {

    private static final long serialVersionUID = 1L;

    public DataAccessException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

}
