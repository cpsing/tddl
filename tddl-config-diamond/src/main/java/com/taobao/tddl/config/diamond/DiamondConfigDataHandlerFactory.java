package com.taobao.tddl.config.diamond;

import java.util.concurrent.Executor;

import com.taobao.diamond.manager.DiamondManager;
import com.taobao.diamond.manager.ManagerListener;
import com.taobao.diamond.manager.impl.DefaultDiamondManager;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

/**
 * @author shenxun
 */
public class DiamondConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    // com.taobao.and_orV0.ANDOR_ONLY_SPEC_APP_NAME_Minus
    public ConfigDataHandler getConfigDataHandler(String appName, String dataId,
                                                  final ConfigDataListener configDataListener) {
        if ((!dataId.startsWith(ConfigDataHandlerFactory.configure_prefix))) {
            StringBuilder sb = new StringBuilder();
            sb.append(ConfigDataHandlerFactory.configure_prefix);
            sb.append(appName).append("_");
            sb.append(dataId);
            dataId = sb.toString();
        }

        DiamondManager dm = new DefaultDiamondManager(dataId, new ManagerListener() {

            public void receiveConfigInfo(String date) {
                configDataListener.onDataRecieved(date);
            }

            public Executor getExecutor() {
                return null;
            }
        });
        return new DiamondConfigDataHandler(dm);
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, final ConfigDataListener configDataListener) {
        DiamondManager dm = new DefaultDiamondManager(dataId, new ManagerListener() {

            public void receiveConfigInfo(String date) {
                configDataListener.onDataRecieved(date);
            }

            public Executor getExecutor() {
                return null;
            }
        });
        return new DiamondConfigDataHandler(dm);
    }

}
