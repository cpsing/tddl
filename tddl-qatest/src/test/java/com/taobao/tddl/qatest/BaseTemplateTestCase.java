package com.taobao.tddl.qatest;

/**
 *  Copyright(c) 2010 taobao. All rights reserved.
 *  通用产品测试
 */

import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Comment for BaseTemplateTestCase
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-11 下午1:57:19
 */

public class BaseTemplateTestCase extends BaseAndorTestCase {

    protected static JdbcTemplate andorJT                    = null;
    protected static final String MATRIX_IBATIS_CONTEXT_PATH = "classpath:ibatis/spring_context.xml";
    protected String              sql                        = null;

    @BeforeClass
    public static void IEnvInit() throws Exception {
        normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        if (us == null) {
            andorJT = JdbcTemplateClient("mysql");
        }
    }

    @Before
    public void init() {
        sql = String.format("delete from %s where pk = ?", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID });

    }
}
