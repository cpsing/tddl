package com.taobao.tddl.optimizer.parse.cobar;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;

/**
 * 基于cobar解析器实现parse
 */
public class CobarSqlParseManager extends AbstractLifecycle implements SqlParseManager {

    private int                                cacheSize  = 1000;
    private long                               expireTime = TddlConstants.DEFAULT_OPTIMIZER_EXPIRE_TIME;
    private static Cache<String, SQLStatement> cache      = null;

    @Override
    protected void doInit() {
        cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(expireTime, TimeUnit.MILLISECONDS)
            .build();
    }

    @Override
    protected void doDestory() {
        cache.invalidateAll();
    }

    @Override
    public SqlAnalysisResult parse(final String sql, boolean cached) throws SqlParserException {
        SQLStatement statement = null;
        try {
            // 只缓存sql的解析结果
            if (cached) {
                statement = cache.get(sql, new Callable<SQLStatement>() {

                    @Override
                    public SQLStatement call() throws Exception {
                        return SQLParserDelegate.parse(sql);
                    }

                });
            } else {
                statement = SQLParserDelegate.parse(sql);
            }
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new SqlParserException("You have an error in your SQL syntax,the sql is:" + sql, e);
            }
        }

        // AstNode visitor结果不能做缓存
        CobarSqlAnalysisResult result = new CobarSqlAnalysisResult();
        result.build(sql, statement);
        return result;

    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

}
