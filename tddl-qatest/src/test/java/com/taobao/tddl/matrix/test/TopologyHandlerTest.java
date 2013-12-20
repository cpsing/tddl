package com.taobao.tddl.matrix.test;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.common.TopologyHandler;

public class TopologyHandlerTest {

    @Test
    public void initTestWithTopologyFileHasAllConfig() {

        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);
        TopologyHandler topology = new TopologyHandler("andor_show", null, "test_matrix.xml");
        // TopologyHandler topology = new TopologyHandler("andor_show", null,
        // null);
        try {
            topology.init();
        } catch (TddlException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void initTestWithTopologyFileHasNoGroupConfig() {

        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);
        TopologyHandler topology = new TopologyHandler("andor_show", null, "test_matrix_without_group_config.xml");
        // TopologyHandler topology = new TopologyHandler("andor_show", null,
        // null);
        try {
            topology.init();
        } catch (TddlException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void initTestWithAppName() {

        ExecutorContext executorContext = new ExecutorContext();
        ExecutorContext.setContext(executorContext);
        TopologyHandler topology = new TopologyHandler("andor_show", null, null);
        // TopologyHandler topology = new TopologyHandler("andor_show", null,
        // null);
        try {
            topology.init();
        } catch (TddlException e) {
            e.printStackTrace();
        }

    }
}
