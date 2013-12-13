package com.taobao.tddl.executor.common;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.executor.ExecutorContext;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;

/**
 * group以及其对应的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.1.0
 */
public class TopologyHandler extends AbstractLifecycle {

    final static Logger                                      logger            = LoggerFactory.getLogger(TopologyHandler.class);
    private final Map<String/* group key */, IGroupExecutor> executorMap       = new HashMap<String, IGroupExecutor>();
    private String                                           appName;
    private String                                           unitName;
    private String                                           topologyFilePath;
    private ConfigDataHandler                                cdh               = null;
    private Matrix                                           matrix;
    public final static MessageFormat                        topologyNullError = new MessageFormat("get topology info error, appName is:{0}, unitName is:{1}, filePath is: {2}, dataId is: {3}");
    public final static MessageFormat                        TOPOLOGY          = new MessageFormat("com.taobao.and_orV0.{0}_MACHINE_TAPOLOGY");

    public TopologyHandler(String appName, String unitName, String topologyFilePath){
        super();
        this.appName = appName;
        this.unitName = unitName;
        this.topologyFilePath = topologyFilePath;
    }

    public Map<String, IGroupExecutor> getExecutorMap() {
        return executorMap;
    }

    protected void doInit() {

        if (topologyFilePath != null) {
            cdh = ConfigDataHandlerCity.getFileFactory(appName).getConfigDataHandler(topologyFilePath,
                new TopologyListener(this));
        } else {
            cdh = ConfigDataHandlerCity.getFactory(appName, unitName)
                .getConfigDataHandler(TOPOLOGY.format(new Object[] { appName }), new TopologyListener(this));
        }
        String data = cdh.getData(ConfigDataHandler.GET_DATA_TIMEOUT, ConfigDataHandler.FIRST_SERVER_STRATEGY);

        if (data == null) {
            String dataid = TOPOLOGY.format(new Object[] { appName });
            throw new TddlRuntimeException(topologyNullError.format(new String[] { appName, unitName, topologyFilePath,
                    dataid }));
        }

        Matrix matrix = MatrixParser.parse(data);

        this.matrix = matrix;

        for (Group group : matrix.getGroups()) {
            group.setAppName(this.appName);
            IRepository repo = ExecutorContext.getContext()
                .getRepositoryHolder()
                .getOrCreateRepository(group.getType().toString());

            IGroupExecutor groupExecutor = repo.buildGroupExecutor(group);

            executorMap.put(group.getName(), groupExecutor);
        }
    }

    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor) {
        return putOne(groupKey, groupExecutor, true);
    }

    public IGroupExecutor putOne(String groupKey, IGroupExecutor groupExecutor, boolean singleton) {
        if (singleton && executorMap.containsKey(groupKey)) {
            throw new IllegalArgumentException("group key is already exists . group key : " + groupKey + " . map "
                                               + executorMap);
        }
        return executorMap.put(groupKey, groupExecutor);
    }

    public IGroupExecutor get(Object key) {
        return executorMap.get(key);
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

    public class TopologyListener implements ConfigDataListener {

        private TopologyHandler topologyHandler;

        public TopologyListener(TopologyHandler topologyHandler){
            this.topologyHandler = topologyHandler;
        }

        @Override
        public void onDataRecieved(String dataId, String data) {
            // TODO Auto-generated method stub

        }

    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public String getTopologyFilePath() {
        return topologyFilePath;
    }

    public void setTopologyFilePath(String topologyFilePath) {
        this.topologyFilePath = topologyFilePath;
    }

    public Matrix getMatrix() {
        return this.matrix;
    }
}
