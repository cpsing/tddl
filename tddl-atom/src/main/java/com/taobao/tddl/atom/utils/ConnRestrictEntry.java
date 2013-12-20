package com.taobao.tddl.atom.utils;

import java.util.Arrays;

import com.taobao.tddl.common.utils.TStringUtil;

/**
 * 应用连接数限制功能中的连接数配置。
 * 
 * @author changyuan.lh
 */
public final class ConnRestrictEntry {

    /**
     * HASH 策略的最大槽数量限制。
     */
    public static final int  MAX_HASH_RESTRICT_SLOT = 32;

    protected final String[] keys;

    protected int            hashSize;

    protected int            limits;

    public ConnRestrictEntry(String[] keys, int hashSize, int limits){
        this.keys = keys;
        this.hashSize = hashSize;
        this.limits = limits;
    }

    public String[] getKeys() {
        return keys;
    }

    public void setHashSize(int hashSize) {
        this.hashSize = hashSize;
    }

    public int getHashSize() {
        return hashSize;
    }

    public void setLimits(int limits) {
        this.limits = limits;
    }

    public int getLimits() {
        return limits;
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + limits;
        result = prime * result + hashSize;
        result = prime * result + Arrays.hashCode(keys);
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ConnRestrictEntry other = (ConnRestrictEntry) obj;
        if (limits != other.limits) return false;
        if (hashSize != other.hashSize) return false;
        if (!Arrays.equals(keys, other.keys)) return false;
        return true;
    }

    public String toString() {
        return "ConnRestrictEntry: " + Arrays.toString(keys) + " " + hashSize + ", " + limits;
    }

    protected static final String KEY_DEFAULT  = "~"; // 匹配 null Key

    protected static final String KEY_WILDCARD = "*"; // 匹配所有 Key

    protected static final String PERCENT      = "%"; // 百分比

    public static boolean isNullKey(String key) {
        return KEY_DEFAULT.equals(key);
    }

    public static boolean isWildcard(String key) {
        return KEY_WILDCARD.equals(key);
    }

    /**
     * Parse "K1,K2,K3:number | *:count,number | ~:number"
     */
    public static ConnRestrictEntry parseEntry(String key, String value, int maxPoolSize) {

        if (key == null || value == null) {
            return null;
        }
        if (KEY_WILDCARD.equals(key)) {
            // Parse "*:count,number"
            int find = value.indexOf(',');
            if (find >= 0) {
                String countStr = value.substring(0, find).trim();
                String numberStr = value.substring(find + 1).trim();
                if (countStr.isEmpty()) {
                    // Parse "*:,number"
                    if (numberStr.endsWith(PERCENT)) {
                        numberStr = TStringUtil.substringBefore(numberStr, PERCENT);
                        if (TStringUtil.isNumeric(numberStr)) {
                            ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                                1,
                                maxPoolSize * Integer.valueOf(numberStr) / 100);
                            return connRestrictEntry;
                        }
                    } else if (TStringUtil.isNumeric(numberStr)) {
                        ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                            1,
                            Integer.valueOf(numberStr));
                        return connRestrictEntry;
                    }
                } else if (TStringUtil.isNumeric(countStr)) {
                    // Parse "*:count,number"
                    if (numberStr.endsWith(PERCENT)) {
                        numberStr = TStringUtil.substringBefore(numberStr, PERCENT);
                        if (TStringUtil.isNumeric(numberStr)) {
                            ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                                Integer.valueOf(countStr),
                                maxPoolSize * Integer.valueOf(numberStr) / 100);
                            return connRestrictEntry;
                        }
                    } else if (TStringUtil.isNumeric(numberStr)) {
                        ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                            Integer.valueOf(countStr),
                            Integer.valueOf(numberStr));
                        return connRestrictEntry;
                    }
                }
            } else {
                // Parse "*:number"
                if (value.endsWith(PERCENT)) {
                    String numberStr = TStringUtil.substringBefore(value, PERCENT);
                    if (TStringUtil.isNumeric(numberStr)) {
                        ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                            1,
                            maxPoolSize * Integer.valueOf(numberStr) / 100);
                        return connRestrictEntry;
                    }
                } else if (TStringUtil.isNumeric(value)) {
                    ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                        1,
                        Integer.valueOf(value));
                    return connRestrictEntry;
                }
            }
        } else if (KEY_DEFAULT.equals(key)) {
            // Parse "~:number"
            if (value.endsWith(PERCENT)) {
                String numberStr = TStringUtil.substringBefore(value, PERCENT);
                if (TStringUtil.isNumeric(numberStr)) {
                    ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                        1,
                        maxPoolSize * Integer.valueOf(numberStr) / 100);
                    return connRestrictEntry;
                }
            } else if (TStringUtil.isNumeric(value)) {
                ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(new String[] { key },
                    1,
                    Integer.valueOf(value));
                return connRestrictEntry;
            }
        } else {
            // Parse "K1,K2,K3:number"
            if (value.endsWith(PERCENT)) {
                String numberStr = TStringUtil.substringBefore(value, PERCENT);
                if (TStringUtil.isNumeric(numberStr)) {
                    String[] keys = TStringUtil.split(key, ",");
                    if (null != keys && keys.length > 0) {
                        ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(keys,
                            1,
                            maxPoolSize * Integer.valueOf(numberStr) / 100);
                        return connRestrictEntry;
                    }
                }
            } else if (TStringUtil.isNumeric(value)) {
                String[] keys = TStringUtil.split(key, ",");
                if (null != keys && keys.length > 0) {
                    ConnRestrictEntry connRestrictEntry = new ConnRestrictEntry(keys, 1, Integer.valueOf(value));
                    return connRestrictEntry;
                }
            }
        }
        return null;
    }
}
