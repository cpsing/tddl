package com.taobao.tddl.executor.exception;

import com.taobao.tddl.common.exception.TddlException;

public class DataAccessException extends TddlException {

    public DataAccessException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
        // TODO Auto-generated constructor stub
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
