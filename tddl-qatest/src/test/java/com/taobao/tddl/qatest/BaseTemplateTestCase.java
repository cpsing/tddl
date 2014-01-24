package com.taobao.tddl.qatest;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Comment for BaseTemplateTestCase
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-11 下午1:57:19
 */
@Ignore(value = "提供初始化环境的实际方法")
public class BaseTemplateTestCase extends BaseMatrixTestCase {

    protected static JdbcTemplate andorJT                    = null;
    protected static final String MATRIX_IBATIS_CONTEXT_PATH = "classpath:spring/spring_context.xml";
    protected String              sql                        = null;

    @BeforeClass
    public static void IEnvInitTemplate() throws Exception {
        normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        if (us == null) {
            andorJT = JdbcTemplateClient("mysql");
        } else {
            andorJT = new JdbcTemplate(us);
        }
    }

    @Before
    public void init() {
        sql = String.format("delete from %s where pk = ?", normaltblTableName);
        andorJT.update(sql, new Object[] { RANDOM_ID });

    }
}
