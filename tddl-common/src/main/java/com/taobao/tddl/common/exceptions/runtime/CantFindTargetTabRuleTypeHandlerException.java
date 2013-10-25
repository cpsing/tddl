package com.taobao.tddl.common.exceptions.runtime;

public class CantFindTargetTabRuleTypeHandlerException extends TDLRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -4073830327289870566L;

    public CantFindTargetTabRuleTypeHandlerException(String msg){
        super("无法找到" + msg + "对应的处理器");
    }
}
