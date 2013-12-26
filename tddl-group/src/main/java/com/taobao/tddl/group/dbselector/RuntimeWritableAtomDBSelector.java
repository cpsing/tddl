package com.taobao.tddl.group.dbselector;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.group.config.GroupConfigManager;
import com.taobao.tddl.group.config.GroupExtraConfig;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;

/**
 * <pre>
 * 用于运行期间主备切换的场景，DBA通常在做完主备切换后才会去修改每个TAtomDataSource的配置。
 * 假设有d0,d1,d2三个库，完成一次切换时对应每个TAtomDataSource的状态快照如下: 
 * ===================== 
 * d0 d1 d2
 * (1) rw r r 
 * (2) na r r 
 * (3) na rw r 
 * (4) r rw r 
 * =====================
 * (1)是切换前正常的状态快照，(4)是切换完成后的状态快照，(2)、(3)是中间过程的状态快照。
 * 如果业务系统在状态(1)、(3)、(4)下要求进行更新操作，则更新操作被允许，因为在这三个状态中都能找到一个含有"w"的db，
 * 如果业务系统在状态(2)下要求进行更新操作，则更新操作被拒绝，抛出异常，因为在这个状态中找不到含有"w"的db，
 * </pre>
 * 
 * @author yangzhu
 */
public class RuntimeWritableAtomDBSelector extends AbstractDBSelector {

    private Map<String, DataSourceWrapper> dataSourceWrapperMap = new HashMap<String, DataSourceWrapper>();
    private EquityDbManager                equityDbManager;

    public RuntimeWritableAtomDBSelector(Map<String, DataSourceWrapper> dataSourceWrapperMap,
                                         GroupExtraConfig groupExtraConfig){
        Map<String, Integer> dummyWeightMap = new HashMap<String, Integer>(dataSourceWrapperMap.size());
        for (String dsKey : dataSourceWrapperMap.keySet())
            dummyWeightMap.put(dsKey, 10);

        this.equityDbManager = new EquityDbManager(dataSourceWrapperMap, dummyWeightMap, groupExtraConfig);
        this.readable = false; // 只用于写
        this.dataSourceWrapperMap = dataSourceWrapperMap;
    }

    public DataSource select() {
        for (Map.Entry<String, DataSourceWrapper> e : dataSourceWrapperMap.entrySet()) {
            if (GroupConfigManager.isDataSourceAvailable(e.getValue(), false)) {
                return e.getValue();
            }
        }
        return null;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Map getDataSources() {
        return dataSourceWrapperMap;
    }

    @Override
    protected <T> T tryExecuteInternal(Map<DataSource, SQLException> failedDataSources, DataSourceTryer<T> tryer,
                                       int times, Object... args) throws SQLException {
        return equityDbManager.tryExecuteInternal(failedDataSources, tryer, times, args);
    }

    public DataSourceWrapper get(String dsKey) {
        return dataSourceWrapperMap.get(dsKey);
    }

    protected DataSourceHolder findDataSourceWrapperByIndex(int dataSourceIndex) {
        return equityDbManager.findDataSourceWrapperByIndex(dataSourceIndex);
    }
}
