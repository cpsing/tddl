package com.taobao.tddl.common.jdbc;

import java.util.regex.Pattern;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.common.utils.TStringUtil;

/**
 * 简单解析sql类型
 * 
 * @author jianghang 2013-10-24 下午4:18:42
 * @since 5.0.0
 */
public class SqlTypeParser {

    /**
     * 用于判断是否是一个select ... for update的sql
     */
    private static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile("^select\\s+.*\\s+for\\s+update.*$",
                                                               Pattern.CASE_INSENSITIVE);

    public static boolean isQuerySql(String sql) {
        SqlType sqlType = getSqlType(sql);
        if (sqlType == SqlType.SELECT || sqlType == SqlType.SELECT_FOR_UPDATE || sqlType == SqlType.SHOW
            || sqlType == SqlType.DUMP || sqlType == SqlType.DEBUG || sqlType == SqlType.EXPLAIN) {
            return true;
        } else if (sqlType == SqlType.INSERT || sqlType == SqlType.UPDATE || sqlType == SqlType.DELETE
                   || sqlType == SqlType.REPLACE || sqlType == SqlType.TRUNCATE || sqlType == SqlType.CREATE
                   || sqlType == SqlType.DROP || sqlType == SqlType.LOAD || sqlType == SqlType.MERGE
                   || sqlType == SqlType.ALTER || sqlType == SqlType.RENAME) {
            return false;
        } else {
            return throwNotSupportSqlTypeException();
        }
    }

    /**
     * 获得SQL语句种类
     * 
     * @param sql SQL语句
     * @throws 当SQL语句不是SELECT、INSERT、UPDATE、DELETE语句时，抛出异常。
     */
    public static SqlType getSqlType(String sql) {
        // #bug 2011-11-24,modify by junyu,先不走缓存，否则sql变化巨大，缓存换入换出太多，gc太明显
        // SqlType sqlType = globalCache.getSqlType(sql);
        // if (sqlType == null) {
        SqlType sqlType = null;
        // #bug 2011-12-8,modify by junyu ,this code use huge cpu resource,and
        // most sql have no comment,so first simple look for there whether have
        // the comment

        String noCommentsSql = sql;
        if (sql.contains("/*")) {
            noCommentsSql = TStringUtil.stripComments(sql, "'\"", "'\"", true, false, true, true).trim();
        }

        if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "select")) {
            // #bug 2011-12-9,this select-for-update regex has low
            // performance,so
            // first judge this sql whether have ' for ' string.
            if (noCommentsSql.toLowerCase().contains(" for ")
                && SELECT_FOR_UPDATE_PATTERN.matcher(noCommentsSql).matches()) {
                sqlType = SqlType.SELECT_FOR_UPDATE;
            } else {
                sqlType = SqlType.SELECT;
            }
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "insert")) {
            sqlType = SqlType.INSERT;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "update")) {
            sqlType = SqlType.UPDATE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "delete")) {
            sqlType = SqlType.DELETE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "show")) {
            sqlType = SqlType.SHOW;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "replace")) {
            sqlType = SqlType.REPLACE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "truncate")) {
            sqlType = SqlType.TRUNCATE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "create")) {
            sqlType = SqlType.CREATE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "drop")) {
            sqlType = SqlType.DROP;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "load")) {
            sqlType = SqlType.LOAD;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "merge")) {
            sqlType = SqlType.MERGE;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "alter")) {
            sqlType = SqlType.ALTER;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "rename")) {
            sqlType = SqlType.RENAME;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "dump")) {
            sqlType = SqlType.DUMP;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "debug")) {
            sqlType = SqlType.DEBUG;
        } else if (TStringUtil.startsWithIgnoreCaseAndWs(noCommentsSql, "explain")) {
            sqlType = SqlType.EXPLAIN;
        } else {
            throwNotSupportSqlTypeException();
        }
        return sqlType;
    }

    public static boolean throwNotSupportSqlTypeException() {
        throw new TddlRuntimeException("only select, insert, update, delete, replace, show, truncate, create, drop, load, merge, dump sql is supported");
    }
}
