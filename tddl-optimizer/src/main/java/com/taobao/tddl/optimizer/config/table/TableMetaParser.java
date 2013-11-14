package com.taobao.tddl.optimizer.config.table;

import java.io.InputStream;
import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;

/**
 * 基于xml定义{@linkplain TableMeta}，对应的解析器
 * 
 * @author jianghang 2013-11-12 下午6:11:17
 * @since 5.1.0
 */
public class TableMetaParser {

    /**
     * 基于数据流创建TableMeta对象
     * 
     * @param in
     * @return
     */
    public static List<TableMeta> parseAll(InputStream in) {
        throw new NotSupportException();
    }

    /**
     * 基于string的data文本创建TableMeta对象
     * 
     * @param data
     * @return
     */
    public static List<TableMeta> parseAll(String data) {
        throw new NotSupportException();
    }
}
