package com.taobao.tddl.group.exception;

//jdk1.5 java.sql.SQLException没有带Throwable cause的构造函数
//public class TAtomDataSourceException extends java.sql.SQLException {

/**
 * @author yangzhu
 */
public class TAtomDataSourceException extends RuntimeException {

    private static final long serialVersionUID = -1L;

    public TAtomDataSourceException(){
        super();
    }

    public TAtomDataSourceException(String msg){
        super(msg);
    }

    public TAtomDataSourceException(Throwable cause){
        super(cause);
    }

    public TAtomDataSourceException(String msg, Throwable cause){
        super(msg, cause);
    }

}
