package com.taobao.tddl.optimizer.config;

import org.junit.Test;

import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;

public class MatrixParserTest {

    @Test
    public void testSimple() {
        Matrix matrix = MatrixParser.parse(Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("config/test_matrix.xml"));
        System.out.println(matrix);
    }
}
