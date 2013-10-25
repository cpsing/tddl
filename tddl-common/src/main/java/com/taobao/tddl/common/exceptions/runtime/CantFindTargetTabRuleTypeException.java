package com.taobao.tddl.common.exceptions.runtime;

public class CantFindTargetTabRuleTypeException extends TDLRunTimeException {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -7179888759169646552L;

    public CantFindTargetTabRuleTypeException(String msg){
        super("无法根据输入的tableRule:" + msg + "找到对应的处理方法。");
    }
}
