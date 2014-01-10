package com.taobao.tddl.optimizer.parse.cobar;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.TddlConstants;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;

/**
 * 基于cobar解析器实现parse
 */
public class CobarSqlParseManager extends AbstractLifecycle implements SqlParseManager {

    private int                                          cacheSize  = 1000;
    private long                                         expireTime = TddlConstants.DEFAULT_OPTIMIZER_EXPIRE_TIME;
    private static Cache<String, CobarSqlAnalysisResult> cache      = null;

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
    public SqlAnalysisResult parse(final String sql, final Map<Integer, ParameterContext> parameterSettings,
                                   boolean cached) throws SqlParserException {
        CobarSqlAnalysisResult result = null;
        try {
            if (cached) {
                result = cache.get(sql, new Callable<CobarSqlAnalysisResult>() {

                    @Override
                    public CobarSqlAnalysisResult call() throws Exception {
                        CobarSqlAnalysisResult bean = new CobarSqlAnalysisResult();
                        bean.parse(sql, parameterSettings);
                        return bean;
                    }

                });
            } else {
                result = new CobarSqlAnalysisResult();
                result.parse(sql, parameterSettings);
            }
        } catch (Exception e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else {
                throw new SqlParserException("You have an error in your SQL syntax,the sql is:" + sql, e);
            }
        }

        return result;

    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

}
