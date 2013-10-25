package com.taobao.tddl.common.exceptions.runtime;

public class InputStringIsNotValidException extends TDLRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 6451499964787923727L;

    public InputStringIsNotValidException(String msg){
        super(msg);
    }
}
