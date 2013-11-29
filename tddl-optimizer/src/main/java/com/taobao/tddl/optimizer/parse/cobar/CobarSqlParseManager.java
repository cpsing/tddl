package com.taobao.tddl.optimizer.parse.cobar;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 基于cobar解析器实现parse
 */
public class CobarSqlParseManager extends AbstractLifecycle implements SqlParseManager {

    private static final Logger                          logger     = LoggerFactory.getLogger(CobarSqlParseManager.class);
    private int                                          cacheSize  = 1000;
    private int                                          expireTime = 30000;
    private static Cache<String, CobarSqlAnalysisResult> cache      = null;

    protected void doInit() {
        cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .expireAfterWrite(expireTime, TimeUnit.MILLISECONDS)
            .build();
    }

    protected void doDestory() {
        cache.invalidateAll();
    }

    public SqlAnalysisResult parse(String sql) throws SqlParserException {
        return parse(sql, true);
    }

    public SqlAnalysisResult parse(final String sql, boolean cached) throws SqlParserException {
        CobarSqlAnalysisResult result = null;
        try {
            if (cached) {
                result = cache.get(sql, new Callable<CobarSqlAnalysisResult>() {

                    public CobarSqlAnalysisResult call() throws Exception {
                        CobarSqlAnalysisResult bean = new CobarSqlAnalysisResult();
                        bean.parse(sql);
                        return bean;
                    }

                });
            } else {
                result = new CobarSqlAnalysisResult();
                result.parse(sql);
            }
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                logger.error("You have an error in your SQL syntax,the sql is:" + sql, e.getCause());
                throw new SqlParserException("You have an error in your SQL syntax,the sql is:" + sql, e.getCause());
            }

        }

        return result;

    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setExpireTime(int expireTime) {
        this.expireTime = expireTime;
    }

}
