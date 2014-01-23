package com.taobao.tddl.common.utils.convertor;

import com.taobao.tddl.common.exception.TddlRuntimeException;

public class ConvertorException extends TddlRuntimeException {

    private static final long serialVersionUID = 3270080439323296921L;

    public ConvertorException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public ConvertorException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public ConvertorException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public ConvertorException(String errorCode){
        super(errorCode);
    }

    public ConvertorException(Throwable cause){
        super(cause);
    }

}
