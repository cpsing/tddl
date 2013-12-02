package com.taobao.tddl.executor.exception;

/**
 * @author mengshi.sunmengshi 2013-11-27 下午3:58:58
 * @since 5.1.0
 */
public class RepositoryException extends Exception {

    public RepositoryException(String message, Throwable cause){
        super(message, cause);
    }

    public RepositoryException(String message){
        super(message);
    }

}
