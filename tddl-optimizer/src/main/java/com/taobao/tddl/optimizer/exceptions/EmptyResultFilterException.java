package com.taobao.tddl.optimizer.exceptions;

/**
 * 空结果的过滤条件异常，比如 0 = 1的条件
 * 
 * @author jianghang 2013-11-13 下午4:05:45
 * @since 5.0.0
 */
public class EmptyResultFilterException extends OptimizerException {

    private static final long serialVersionUID = -7525463650321091760L;

    public EmptyResultFilterException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public EmptyResultFilterException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public EmptyResultFilterException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public EmptyResultFilterException(String errorCode){
        super(errorCode);
    }

    public EmptyResultFilterException(Throwable cause){
        super(cause);
    }

}
