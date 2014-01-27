package com.taobao.tddl.common.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * Tddl nestabled {@link RuntimeException}
 * 
 * @author jianghang 2013-10-24 下午2:55:22
 * @since 5.0.0
 */
public class TddlRuntimeException extends NestableRuntimeException {

    private static final long serialVersionUID = -654893533794556357L;

    public TddlRuntimeException(String errorCode){
        super(errorCode);
    }

    public TddlRuntimeException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public TddlRuntimeException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public TddlRuntimeException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public TddlRuntimeException(Throwable cause){
        super(cause);
    }

}
