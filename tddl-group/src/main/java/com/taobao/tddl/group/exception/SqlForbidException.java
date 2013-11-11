package com.taobao.tddl.group.exception;

/**
 * @author jiechen.qzm SQL WITCH IS IN FORBIDDEN SET
 */
public class SqlForbidException extends RuntimeException {

    private static final long serialVersionUID = -1L;

    public SqlForbidException(){
        super();
    }

    public SqlForbidException(String msg){
        super(msg);
    }

    public SqlForbidException(Throwable cause){
        super(cause);
    }

    public SqlForbidException(String msg, Throwable cause){
        super(msg, cause);
    }
}
