package com.taobao.tddl.group.exception;

/**
 * @author yangzhu
 */
public class ConfigException extends RuntimeException {

    private static final long serialVersionUID = -1L;

    public ConfigException(){
        super();
    }

    public ConfigException(String msg){
        super(msg);
    }

    public ConfigException(Throwable cause){
        super(cause);
    }

    public ConfigException(String msg, Throwable cause){
        super(msg, cause);
    }

}
