package com.taobao.tddl.rule.exceptions;

import com.taobao.tddl.common.exception.TddlException;

/**
 * 多版本route不匹配
 * 
 * @author jianghang 2013-11-5 下午1:40:43
 * @since 5.0.0
 */
public class RouteCompareDiffException extends TddlException {

    private static final long serialVersionUID = 7019507989647341019L;

    public RouteCompareDiffException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode, errorDesc, cause);
    }

    public RouteCompareDiffException(String errorCode, String errorDesc){
        super(errorCode, errorDesc);
    }

    public RouteCompareDiffException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public RouteCompareDiffException(String errorCode){
        super(errorCode);
    }

    public RouteCompareDiffException(Throwable cause){
        super(cause);
    }

}
