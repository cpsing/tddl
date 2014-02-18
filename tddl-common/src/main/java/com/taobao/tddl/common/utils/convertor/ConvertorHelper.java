package com.taobao.tddl.common.utils.convertor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;

/**
 * convert转化helper类，注册一些默认的convertor
 * 
 * @author jianghang 2011-5-20 下午04:44:38
 */
public class ConvertorHelper {

    // common对象范围：8种Primitive和对应的Java类型，BigDecimal, BigInteger
    public static Map<Class, Object>        commonTypes            = new HashMap<Class, Object>();
    public static final Convertor           stringToCommon         = new StringAndCommonConvertor.StringToCommon();
    public static final Convertor           commonToCommon         = new CommonAndCommonConvertor.CommonToCommon();
    // toString处理
    public static final Convertor           objectToString         = new StringAndObjectConvertor.ObjectToString();
    // 数组处理
    private static final Convertor          arrayToArray           = new CollectionAndCollectionConvertor.ArrayToArray();
    private static final Convertor          arrayToCollection      = new CollectionAndCollectionConvertor.ArrayToCollection();
    private static final Convertor          collectionToArray      = new CollectionAndCollectionConvertor.CollectionToArray();
    private static final Convertor          collectionToCollection = new CollectionAndCollectionConvertor.CollectionToCollection();
    // 枚举处理
    public static final Convertor           stringToEnum           = new StringAndEnumConvertor.StringToEnum();
    public static final Convertor           enumToString           = new StringAndEnumConvertor.EnumToString();
    public static final Convertor           sqlToDate              = new SqlDateAndDateConvertor.SqlDateToDateConvertor();
    public static final Convertor           dateToSql              = new SqlDateAndDateConvertor.DateToSqlDateConvertor();
    public static final Convertor           blobToBytes            = new BlobAndBytesConvertor.BlobToBytes();
    public static final Convertor           stringToBytes          = new StringAndObjectConvertor.StringToBytes();
    public static final Convertor           bytesToString          = new StringAndObjectConvertor.BytesToString();

    private static volatile ConvertorHelper singleton              = null;

    private ConvertorRepository             repository             = null;

    public ConvertorHelper(){
        repository = new ConvertorRepository();
        initDefaultRegister();
    }

    public ConvertorHelper(ConvertorRepository repository){
        // 允许传入自定义仓库
        this.repository = repository;
        initDefaultRegister();
    }

    /**
     * 单例方法
     */
    public static ConvertorHelper getInstance() {
        if (singleton == null) {
            synchronized (ConvertorHelper.class) {
                if (singleton == null) { // double check
                    singleton = new ConvertorHelper();
                }
            }
        }
        return singleton;
    }

    /**
     * 根据class获取对应的convertor
     * 
     * @return
     */
    public Convertor getConvertor(Class src, Class dest) {
        if (src == dest) {
            // 对相同类型的直接忽略，不做转换
            return null;
        }

        // 按照src->dest来取映射
        Convertor convertor = repository.getConvertor(src, dest);

        // 如果dest是string，获取一下object->string.
        // (系统默认注册了一个Object.class -> String.class的转化)
        if (convertor == null && dest == String.class) {
            if (src.isEnum()) {// 如果是枚举
                convertor = enumToString;
            } else { // 默认进行toString输出
                convertor = objectToString;
            }
        }

        // 处理下Array|Collection的映射
        // 如果src|dest是array类型，取一下Array.class的映射，因为默认数组处理的注册直接注册了Array.class
        boolean isSrcArray = src.isArray();
        boolean isDestArray = dest.isArray();
        if (convertor == null && src.isArray() && dest.isArray()) {
            convertor = arrayToArray;
        } else {
            boolean isSrcCollection = Collection.class.isAssignableFrom(src);
            boolean isDestCollection = Collection.class.isAssignableFrom(dest);
            if (convertor == null && isSrcArray && isDestCollection) {
                convertor = arrayToCollection;
            }
            if (convertor == null && isDestArray && isSrcCollection) {
                convertor = collectionToArray;
            }
            if (convertor == null && isSrcCollection && isDestCollection) {
                convertor = collectionToCollection;
            }
        }

        // 如果是其中一个是String类
        if (convertor == null && src == String.class) {
            if (commonTypes.containsKey(dest)) { // 另一个是Common类型
                convertor = stringToCommon;
            } else if (dest.isEnum()) { // 另一个是枚举对象
                convertor = stringToEnum;
            }
        }

        // 如果src/dest都是Common类型，进行特殊处理
        if (convertor == null && commonTypes.containsKey(src) && commonTypes.containsKey(dest)) {
            convertor = commonToCommon;
        }

        return convertor;
    }

    /**
     * 根据alias获取对应的convertor
     * 
     * @return
     */
    public Convertor getConvertor(String alias) {
        return repository.getConvertor(alias);
    }

    /**
     * 注册class对应的convertor
     */
    public void registerConvertor(Class src, Class dest, Convertor convertor) {
        repository.registerConvertor(src, dest, convertor);
    }

    /**
     * 注册alias对应的convertor
     */
    public void registerConvertor(String alias, Convertor convertor) {
        repository.registerConvertor(alias, convertor);
    }

    // ======================= register处理 ======================

    public void initDefaultRegister() {
        initCommonTypes();
        StringDateRegister();
    }

    private void StringDateRegister() {
        // 注册string<->date对象处理
        Convertor stringToDate = new StringAndDateConvertor.StringToDate();
        Convertor stringToCalendar = new StringAndDateConvertor.StringToCalendar();
        Convertor stringToSqlDate = new StringAndDateConvertor.StringToSqlDate();
        Convertor sqlDateToString = new StringAndDateConvertor.SqlDateToString();
        Convertor sqlTimeToString = new StringAndDateConvertor.SqlTimeToString();
        Convertor sqlTimestampToString = new StringAndDateConvertor.SqlTimestampToString();
        Convertor calendarToString = new StringAndDateConvertor.CalendarToString();
        Convertor longToDate = new LongAndDateConvertor.LongToDateConvertor();
        Convertor dateToLong = new LongAndDateConvertor.DateToLongConvertor();
        // 注册默认的String <-> Date的处理
        repository.registerConvertor(String.class, Date.class, stringToDate);
        repository.registerConvertor(String.class, Calendar.class, stringToCalendar);
        repository.registerConvertor(String.class, java.sql.Date.class, stringToSqlDate);
        repository.registerConvertor(String.class, java.sql.Time.class, stringToSqlDate);
        repository.registerConvertor(String.class, java.sql.Timestamp.class, stringToSqlDate);
        // 如果是date，默认用timestamp
        repository.registerConvertor(java.util.Date.class, String.class, sqlTimestampToString);
        repository.registerConvertor(java.sql.Date.class, String.class, sqlDateToString);
        repository.registerConvertor(java.sql.Time.class, String.class, sqlTimeToString);
        repository.registerConvertor(java.sql.Timestamp.class, String.class, sqlTimestampToString);
        repository.registerConvertor(Calendar.class, String.class, calendarToString);
        repository.registerConvertor(java.util.Date.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Date.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Time.class, Long.class, dateToLong);
        repository.registerConvertor(java.sql.Timestamp.class, Long.class, dateToLong);

        repository.registerConvertor(Long.class, java.util.Date.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Date.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Time.class, longToDate);
        repository.registerConvertor(Long.class, java.sql.Timestamp.class, longToDate);
        // 注册默认的Date <-> SqlDate的处理
        repository.registerConvertor(java.sql.Date.class, Date.class, sqlToDate);
        repository.registerConvertor(java.sql.Time.class, Date.class, sqlToDate);
        repository.registerConvertor(java.sql.Timestamp.class, Date.class, sqlToDate);
        repository.registerConvertor(Date.class, java.sql.Date.class, dateToSql);
        repository.registerConvertor(Date.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(Date.class, java.sql.Timestamp.class, dateToSql);
        repository.registerConvertor(java.sql.Timestamp.class, java.sql.Date.class, dateToSql);
        repository.registerConvertor(java.sql.Timestamp.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(java.sql.Date.class, java.sql.Timestamp.class, dateToSql);
        repository.registerConvertor(java.sql.Date.class, java.sql.Time.class, dateToSql);
        repository.registerConvertor(java.sql.Time.class, java.sql.Timestamp.class, dateToSql);
        repository.registerConvertor(java.sql.Time.class, java.sql.Date.class, dateToSql);
        repository.registerConvertor(Blob.class, byte[].class, blobToBytes);
        repository.registerConvertor(String.class, byte[].class, stringToBytes);
        repository.registerConvertor(byte[].class, String.class, bytesToString);
    }

    private void initCommonTypes() {
        commonTypes.put(int.class, ObjectUtils.NULL);
        commonTypes.put(Integer.class, ObjectUtils.NULL);
        commonTypes.put(short.class, ObjectUtils.NULL);
        commonTypes.put(Short.class, ObjectUtils.NULL);
        commonTypes.put(long.class, ObjectUtils.NULL);
        commonTypes.put(Long.class, ObjectUtils.NULL);
        commonTypes.put(boolean.class, ObjectUtils.NULL);
        commonTypes.put(Boolean.class, ObjectUtils.NULL);
        commonTypes.put(byte.class, ObjectUtils.NULL);
        commonTypes.put(Byte.class, ObjectUtils.NULL);
        commonTypes.put(char.class, ObjectUtils.NULL);
        commonTypes.put(Character.class, ObjectUtils.NULL);
        commonTypes.put(float.class, ObjectUtils.NULL);
        commonTypes.put(Float.class, ObjectUtils.NULL);
        commonTypes.put(double.class, ObjectUtils.NULL);
        commonTypes.put(Double.class, ObjectUtils.NULL);
        commonTypes.put(BigDecimal.class, ObjectUtils.NULL);
        commonTypes.put(BigInteger.class, ObjectUtils.NULL);
    }

    // ========================= setter / getter ===================

    public void setRepository(ConvertorRepository repository) {
        this.repository = repository;
    }
}
