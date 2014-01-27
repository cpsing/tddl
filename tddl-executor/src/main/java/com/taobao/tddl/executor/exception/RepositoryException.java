package com.taobao.tddl.executor.exception;

import com.taobao.tddl.common.exception.TddlException;

/**
 * @author mengshi.sunmengshi 2013-11-27 下午3:58:58
 * @since 5.0.0
 */
public class RepositoryException extends TddlException {

    private static final long serialVersionUID = 6716562075578894547L;

    public RepositoryException(String message, Throwable cause){
        super(message, cause);
    }

    public RepositoryException(String message){
        super(message);
    }

}
