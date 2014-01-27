package com.taobao.tddl.common.exception;

import org.apache.commons.lang.exception.NestableException;

/**
 * Tddl nestabled {@link Exception}
 * 
 * @author jianghang 2013-10-24 下午2:55:38
 * @since 5.0.0
 */
public class TddlException extends NestableException {

    private static final long serialVersionUID = 1540164086674285095L;

    public TddlException(String errorCode){
        super(errorCode);
    }

    public TddlException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public TddlException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public TddlException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public TddlException(Throwable cause){
        super(cause);
    }

    public TddlException(int errorCode, String errorDesc){
        this(String.valueOf(errorCode), errorDesc);
    }

    public TddlException(int errorCode, Throwable cause){
        this(String.valueOf(errorCode), cause);
    }
}
