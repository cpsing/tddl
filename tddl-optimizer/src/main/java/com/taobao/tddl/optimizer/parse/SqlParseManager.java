package com.taobao.tddl.optimizer.parse;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.optimizer.exceptions.SqlParserException;

/**
 * 基于sql构建语法树
 * 
 * @author jianghang 2013-11-12 下午2:30:20
 * @since 5.0.0
 */
public interface SqlParseManager extends Lifecycle {

    public SqlAnalysisResult parse(final String sql, boolean cached) throws SqlParserException;
}
