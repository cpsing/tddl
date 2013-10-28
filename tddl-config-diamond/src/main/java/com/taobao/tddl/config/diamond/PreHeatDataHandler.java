package com.taobao.tddl.config.diamond;

import com.taobao.tddl.common.utils.StringUtils;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.config.ConfigHolderFactory;

/**
 * @author JIECHEN
 */
public class PreHeatDataHandler extends DiamondConfigDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(PreHeatDataHandler.class);

    private String              appName;

    public PreHeatDataHandler(String appName){
        this.appName = appName;
    }

    public String getData(long timeout, String strategy) {
        if (ConfigHolderFactory.isInit(appName)) {
            String result = ConfigHolderFactory.getConfigDataHolder(appName).getData(dataId);
            if (!StringUtils.nullOrEmpty(result)) {
                return result;
            }
            logger.error("PreHeatDataHandler Miss Data, Use Default Handler. DataId Is : " + dataId);
        }
        return super.getData(timeout, strategy);
    }

    public String getNullableData(long timeout, String strategy) {
        if (ConfigHolderFactory.isInit(appName)) {
            return ConfigHolderFactory.getConfigDataHolder(appName).getData(dataId);
        }
        return super.getData(timeout, strategy);
    }

}
