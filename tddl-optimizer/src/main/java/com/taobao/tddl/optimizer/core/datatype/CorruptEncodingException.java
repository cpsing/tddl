package com.taobao.tddl.optimizer.core.datatype;

import com.taobao.tddl.common.exception.TddlException;

public class CorruptEncodingException extends TddlException {

    private static final long serialVersionUID = -7798002309588878953L;

    public CorruptEncodingException(String message){
        super(message);
    }

    public CorruptEncodingException(String message, Throwable cause){
        super(message, cause);
    }

    public CorruptEncodingException(Throwable cause){
        super(cause);
    }
}
