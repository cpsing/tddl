package com.taobao.tddl.common.exceptions.runtime;

public class CantFindTargetVirtualNameException extends TDLRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 5542737179921749267L;

    public CantFindTargetVirtualNameException(String virtualTabName){
        super("can't find virtualTabName:" + virtualTabName);
    }
}
