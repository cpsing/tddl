package com.taobao.tddl.group.exception;

/**
 * @author yangzhu
 */
public class TGroupDataSourceException extends RuntimeException {

    private static final long serialVersionUID = -1L;

    public TGroupDataSourceException(){
        super();
    }

    public TGroupDataSourceException(String msg){
        super(msg);
    }

    public TGroupDataSourceException(Throwable cause){
        super(cause);
    }

    public TGroupDataSourceException(String msg, Throwable cause){
        super(msg, cause);
    }

}
