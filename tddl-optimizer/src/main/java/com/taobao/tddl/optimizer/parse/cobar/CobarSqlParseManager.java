package com.taobao.tddl.optimizer.parse.cobar;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.SqlParseManager;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 基于cobar解析器实现parse
 */
public class CobarSqlParseManager implements SqlParseManager {

    private static final Logger                          logger = LoggerFactory.getLogger(CobarSqlParseManager.class);
    private static Cache<String, CobarSqlAnalysisResult> cache  = CacheBuilder.newBuilder()
                                                                    .maximumSize(1000)
                                                                    .expireAfterWrite(30000, TimeUnit.MILLISECONDS)
                                                                    .build();

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

}
