package com.taobao.tddl.group.exception;

import java.sql.SQLException;

/**
 * 当一组的数据库都试过，都不可用了，并且没有更多的数据源了，抛出该错误
 * 
 * @author linxuan
 */
public class NoMoreDataSourceException extends SQLException {

    private static final long serialVersionUID = 1L;

    public NoMoreDataSourceException(String reason){
        super(reason
              + "\nPlease grep 'createSQLException' or 'createCommunicationsException' or 'time out' to find real causes."
              + " And you can reference " + "'http://baike.corp.taobao.com/index.php/TDDL_FAQ'");
    }

}
