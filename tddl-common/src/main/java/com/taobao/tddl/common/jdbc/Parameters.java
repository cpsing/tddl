package com.taobao.tddl.common.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author yangzhu
 */
public class Parameters {

    public static void setParameters(PreparedStatement ps, Map<Integer, ParameterContext> parameterSettings)
                                                                                                            throws SQLException {
        ParameterMethod.setParameters(ps, parameterSettings);
    }

}
