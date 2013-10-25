package com.taobao.tddl.common.exceptions.runtime;

public class TDLRunTimeException extends RuntimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -2139691156552402165L;

    public TDLRunTimeException(String arg){
        super(arg);
    }

    public TDLRunTimeException(){
        super();
    }

    public TDLRunTimeException(String message, Throwable cause){
        super(message, cause);
    }

    public TDLRunTimeException(Throwable throwable){
        super(throwable);
    }
}
