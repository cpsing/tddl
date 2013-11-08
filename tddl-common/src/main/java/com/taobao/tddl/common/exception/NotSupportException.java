package com.taobao.tddl.common.exception;

public class NotSupportException extends TddlRuntimeException {

    private static final long serialVersionUID = 3333002727706650503L;

    public NotSupportException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public NotSupportException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public NotSupportException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public NotSupportException(String errorCode){
        super(errorCode);
    }

    public NotSupportException(Throwable cause){
        super(cause);
    }

    public NotSupportException(){
        super("not support yet!");
    }
}
