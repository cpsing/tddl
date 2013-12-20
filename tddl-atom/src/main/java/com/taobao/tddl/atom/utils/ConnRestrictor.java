package com.taobao.tddl.atom.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.taobao.tddl.atom.exception.AtomNotAvailableException;
import com.taobao.tddl.common.utils.TStringUtil;

/**
 * 应用连接限制的主要逻辑实现。
 */
public class ConnRestrictor {

    /**
     * MAP 策略的应用连接限制, 精确的匹配 Key 和连接槽。
     */
    private HashMap<String, ConnRestrictSlot> mapConnRestrict;

    /**
     * HASH 策略的应用连接限制, 用 Hash + 取模的方式匹配 Key 和连接槽。
     */
    private ConnRestrictSlot[]                hashConnRestrict;

    /**
     * 没有定义业务键 (null Key) 的连接限制槽。
     */
    private ConnRestrictSlot                  nullKeyRestrictSlot;

    /**
     * 初始化应用连接限制的数据结构, 这些数据结构只会被初始化一次。
     */
    public ConnRestrictor(String datasourceKey, List<ConnRestrictEntry> connRestrictEntries){
        for (ConnRestrictEntry connRestrictEntry : connRestrictEntries) {
            String[] slotKeys = connRestrictEntry.getKeys();
            if (slotKeys.length == 1 && ConnRestrictEntry.isWildcard(slotKeys[0])) {
                int maxHashSize = connRestrictEntry.getHashSize();
                if (maxHashSize < 1) {
                    maxHashSize = 1;
                }
                if (maxHashSize > ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT) {
                    maxHashSize = ConnRestrictEntry.MAX_HASH_RESTRICT_SLOT;
                }
                if (hashConnRestrict == null) {
                    // 每个 HASH 分片都用独立的槽
                    hashConnRestrict = new ConnRestrictSlot[maxHashSize];
                    for (int i = 0; i < maxHashSize; i++) {
                        hashConnRestrict[i] = new ConnRestrictSlot(datasourceKey, "*:" + i, connRestrictEntry);
                    }
                }
            } else if (slotKeys.length == 1 && ConnRestrictEntry.isNullKey(slotKeys[0])) {
                if (nullKeyRestrictSlot == null) {
                    nullKeyRestrictSlot = new ConnRestrictSlot(datasourceKey, slotKeys[0], connRestrictEntry);
                }
            } else {
                // 注意, 这里多个业务键同时关联到一个槽
                ConnRestrictSlot connRestrictSlot = new ConnRestrictSlot(datasourceKey,
                    TStringUtil.join(slotKeys, '|'),
                    connRestrictEntry);
                if (mapConnRestrict == null) {
                    mapConnRestrict = new HashMap<String, ConnRestrictSlot>();
                }
                for (String slotKey : slotKeys) {
                    if (ConnRestrictEntry.isNullKey(slotKey)) {
                        if (nullKeyRestrictSlot == null) {
                            nullKeyRestrictSlot = connRestrictSlot;
                        }
                    } else if (!ConnRestrictEntry.isWildcard(slotKey)) {
                        if (!mapConnRestrict.containsKey(slotKey)) {
                            mapConnRestrict.put(slotKey, connRestrictSlot);
                        }
                    }
                }
            }
        }
    }

    /**
     * 从数据结构中查找应用连接限制的槽。
     */
    public ConnRestrictSlot findSlot(Object connKey) {
        if (connKey != null) {
            ConnRestrictSlot connRestrictSlot = null;
            if (mapConnRestrict != null) {
                // 首先精确匹配
                connRestrictSlot = mapConnRestrict.get(String.valueOf(connKey));
            }
            if (connRestrictSlot == null) {
                if (hashConnRestrict != null) {
                    // 如果没有精确指定, 则用 HASH 方式
                    final int hash = Math.abs(connKey.hashCode() % hashConnRestrict.length);
                    connRestrictSlot = hashConnRestrict[hash];
                }
            }
            return connRestrictSlot;
        }
        // 没有定义业务键, 走 null Key 槽
        return nullKeyRestrictSlot;
    }

    /**
     * 应用连接限制的入口函数。
     */
    public ConnRestrictSlot doRestrict(final int timeoutInMillis) throws SQLException {
        final Object connKey = AtomDataSourceHelper.getConnRestrictKey();
        ConnRestrictSlot connRestrictSlot = findSlot(connKey);
        try {
            // 如果没有匹配的槽, 忽略限制
            if (connRestrictSlot != null) {
                if (!connRestrictSlot.allocateConnection(timeoutInMillis)) {
                    // 阻塞超时
                    throw new AtomNotAvailableException("No connection available for '" + connKey
                                                        + "' within configured blocking timeout (" + timeoutInMillis
                                                        + "[ms])");
                }
            }
        } catch (InterruptedException e) {
            throw new AtomNotAvailableException("Allocate connection for '" + connKey
                                                + "' interrupted within configured blocking timeout ("
                                                + timeoutInMillis + "[ms]) , caused by "
                                                + ExceptionUtils.getFullStackTrace(e));
        } catch (RuntimeException e) {
            throw new AtomNotAvailableException("Allocate connection for '" + connKey
                                                + "' failed: unexpected exception "
                                                + ExceptionUtils.getFullStackTrace(e));
        }
        return connRestrictSlot;
    }
}
