package com.taobao.tddl.executor.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.Group.GroupType;
import com.taobao.tddl.common.model.Matrix;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.utils.XmlHelper;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.ConfigDataHandlerCity;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.table.parse.MatrixParser;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * group以及其对应的执行器
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:33
 * @since 5.0.0
 */
public class TopologyHandler extends AbstractLifecycle {

    public final static Logger                               logger            = LoggerFactory.getLogger(TopologyHandler.class);
    public final static String                               xmlHead           = "<matrix xmlns=\"https://github.com/tddl/tddl/schema/matrix\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"https://github.com/tddl/tddl/schema/matrix https://raw.github.com/tddl/tddl/master/tddl-common/src/main/resources/META-INF/matrix.xsd\">";
    public final static MessageFormat                        topologyNullError = new MessageFormat("get topology info error, appName is:{0}, unitName is:{1}, filePath is: {2}, dataId is: {3}");
    public final static MessageFormat                        TOPOLOGY          = new MessageFormat("com.taobao.and_orV0.{0}_MACHINE_TAPOLOGY");
    private final Map<String/* group key */, IGroupExecutor> executorMap       = new HashMap<String, IGroupExecutor>();
    private String                                           appName;
    private String                                           unitName;
    private String                                           topologyFilePath;
    private ConfigDataHandler                                cdh               = null;
    private Matrix                                           matrix;

    public TopologyHandler(String appName, String unitName, String topologyFilePath){
        this.appName = appName;
        this.unitName = unitName;
        this.topologyFilePath = topologyFilePath;
    }

    public Map<String, IGroupExecutor> getExecutorMap() {
        return executorMap;
    }

    @Override
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
            // 尝试读一次tddl的appName规则
            data = generateTopologyXML(appName, unitName);
        }

        if (data == null) {
            String dataId = TOPOLOGY.format(new Object[] { appName });
            throw new TddlRuntimeException(topologyNullError.format(new String[] { appName, unitName, topologyFilePath,
                    dataId }));
        }

        try {
            Matrix matrix = MatrixParser.parse(data);
            this.matrix = matrix;
        } catch (Exception ex) {
            logger.error("matrix topology init error,file is: " + this.getTopologyFilePath() + ", appname is: "
                         + this.getAppName(),
                ex);
            throw new TddlRuntimeException(ex);
        }

        for (Group group : matrix.getGroups()) {
            group.setAppName(this.appName);
            IRepository repo = ExecutorContext.getContext()
                .getRepositoryHolder()
                .getOrCreateRepository(group.getType().toString(), matrix.getProperties());

            IGroupExecutor groupExecutor = repo.getGroupExecutor(group);
            executorMap.put(group.getName(), groupExecutor);
        }
    }

    protected void doDestory() throws TddlException {
        Map<String, IRepository> repos = ExecutorContext.getContext().getRepositoryHolder().getRepository();
        for (IRepository repo : repos.values()) {
            repo.destory();
        }

        cdh.destory();
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

    private String generateTopologyXML(String appName, String unitName) {
        try {
            String matrixKey = "com.taobao.tddl.v1_" + appName + "_dbgroups";
            ConfigDataHandler groupHanlder = ConfigDataHandlerCity.getFactory(appName, unitName)
                .getConfigDataHandler(matrixKey);
            String keys = groupHanlder.getData(ConfigDataHandler.GET_DATA_TIMEOUT,
                ConfigDataHandler.FIRST_SERVER_STRATEGY);
            if (keys == null) {
                throw new IllegalArgumentException("dataId : " + matrixKey + " is null");
            }

            String[] keysArray = keys.split(",");
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = dbf.newDocumentBuilder();
            Document doc = builder.newDocument();

            Element matrix = doc.createElement("matrix");
            // matrix.setAttribute("name", appName);
            doc.appendChild(matrix); // 将根元素添加到文档上

            for (String str : keysArray) {
                Element group = doc.createElement("group");
                group.setAttribute("name", str);
                group.setAttribute("type", GroupType.MYSQL_JDBC.name());// 默认为mysql类型
                matrix.appendChild(group);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamWriter outwriter = new OutputStreamWriter(baos);
            XmlHelper.callWriteXmlFile(doc, outwriter, "utf-8");
            outwriter.close();
            String xml = baos.toString();
            return StringUtils.replace(xml, "<matrix>", xmlHead);
        } catch (IOException e) {
            throw new TddlRuntimeException(e);
        } catch (ParserConfigurationException e) {
            throw new TddlRuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "TopologyHandler [executorMap=" + executorMap + "]";
    }

    public class TopologyListener implements ConfigDataListener {

        @SuppressWarnings("unused")
        private final TopologyHandler topologyHandler;

        public TopologyListener(TopologyHandler topologyHandler){
            this.topologyHandler = topologyHandler;
        }

        @Override
        public void onDataRecieved(String dataId, String data) {
            // do nothing
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
