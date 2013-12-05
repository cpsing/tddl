package com.taobao.tddl.executor.cursor;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;

/**
 * @author jianxing <jianxing.qx@taobao.com> 注解添加by danchen
 */
public interface Cursor {

    /**
     * 根据指定的key跳到指定的游标位置 如果发现匹配数据，则返回true; 否则返回false 指针跳过去的同时，current对象也进行更新。
     */
    boolean skipTo(CloneableRecord key) throws TddlException;

    /**
     * 根据指定的key+val跳到指定的游标位置 这个skipto ，要根据KVPair里面的key和val进行查找，同时匹配的时候，才会返回true.
     * 否则返回false 指针跳过去的同时，current对象也进行更新。 随机读
     */
    boolean skipTo(KVPair key) throws TddlException;

    /*
     * 当前行的游标位置
     */
    IRowSet current() throws TddlException;

    /**
     * 下一行的游标位置
     * <P>
     * 从实现来说，如果游标未初始化，那么初始化游标并将他放置在结果集的第一个位置上
     * </p>
     * <p>
     * 如果当前映射中有重复的key的数据（比如索引，一个用户有多个商品），那么会导致指针下移，但value可能会变更 如 map : 1->0 ;
     * 1->1 ; 1->2 ; 1->3 那么next会便利所有的1对应的数据，依次返回1->0, 1->1,1->2,1->3
     * </p>
     * <p>
     * next也会让[current数据]指向当前值。比如next() 返回的是 1->0 那么再次调用current ，返回的也将是1->0
     * </p>
     */
    IRowSet next() throws TddlException;

    /**
     * 上一行的游标位置
     * <P>
     * 从实现来说，如果游标未初始化，那么初始化游标并将他放置在结果集的最底端位置上
     * </p>
     * <p>
     * 如果当前映射中有重复的key的数据（比如索引，一个用户有多个商品），那么会导致指针下移，但value可能会变更 如 map : 1->0 ;
     * 1->1 ; 1->2 ; 1->3 那么next会便利所有的1对应的数据，依次返回1->3, 1->2,1->1,1->0
     * </p>
     * <p>
     * prev也会让[current数据]指向当前值。比如next() 返回的是 1->0 那么再次调用current ，返回的也将是1->0
     * </p>
     */
    IRowSet prev() throws TddlException;

    /**
     * 结果集的第一行的游标位置
     * <p>
     * 也会让[current数据]指向当前值。比如next() 返回的是 1->0 那么再次调用current ，返回的也将是1->0
     * </p>
     */
    IRowSet first() throws TddlException;

    /**
     * 结果集的第一行之前的游标位置
     */
    void beforeFirst() throws TddlException;

    /**
     * 游标指向结果集的最后一行的游标位置 也会让[current数据]指向当前值。比如next() 返回的是 1->0 那么再次调用current
     * ，返回的也将是1->0
     */
    IRowSet last() throws TddlException;

    /**
     * 根据key获得结果集中的整行记录KVPair 将游标跳转到当前结果上。
     * 如果有相同的key的数据[一对多索引情况]，那么跳转到当前key的第一个数据上 也会让[current数据]指向当前值。比如next() 返回的是
     * 1->0 那么再次调用current ，返回的也将是1->0 目前没有被使用。注释掉
     */
    // KVPair get(CloneableRecord key)throws Exception;

    /**
     * 根据key获得结果集中的整行记录KVPair 同get(CloneableRecord key) 目前没有被使用。注释掉
     */
    // KVPair get(KVPair key)throws Exception;

    /**
     * 关闭结果集游标
     */
    List<TddlException> close(List<TddlException> exceptions);

    /**
     * 删除 如果当前游标有数据，那么删除这个游标所指向的数据
     */
    boolean delete() throws TddlException;

    /**
     * 获取一个相同的值。 如果下一个值为不同值，则返回空 也会让[current数据]指向当前值。比如next() 返回的是 1->0
     * 那么再次调用current ，返回的也将是1->0 简单来说 你在指针跳转的时候，只会利用key跳到值的第一个上。 比如 [0->0, 0->1
     * , 0->2,1->0,1->1] 如果skipTo 0，那么指针只会在0->0这里 但第一个值之后，可能有重复的啊。
     * 为了取尽0->0,0->1,0->2 就需要一个接口，如果key相同的还有数据，那么就取出，如果下一个key不同了，就返回空
     */
    IRowSet getNextDup() throws TddlException;

    /*
     * 添加
     */
    void put(CloneableRecord key, CloneableRecord value) throws TddlException;

    /**
     * 批量取数据的接口,返回是map,所以会有一次hash的开销 不会导致指针发生变动。 会将有相同数据的值全部取出,[按照给定keys的顺序返回]
     * 比如一个索引， [0->0, 0->1 , 0->2...]
     * 那么当mget(0)的时候，应该返回[1],[2]...所有与0相关的值，但这个id是否排序，要看下层实现是否排序。
     * 如果有未找到的值，那么会向Map中插入空。
     * 
     * @param keys
     * @param keyFilterOrValueFilter 为true使用keyFilter，为false使用valueFilter
     * @return
     * @throws Exception
     */
    Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                            boolean keyFilterOrValueFilter) throws TddlException;

    /**
     * 批量取数据的接口，返回是list,少了一次hash的开销 不会导致指针发生变动。 会将有相同数据的值全部取出,[按照给定keys的顺序返回]
     * 比如一个索引， [0->0, 0->1 , 0->2...]
     * 那么当mget(0)的时候，应该返回[1],[2]...所有与0相关的值，但这个id是否排序，要看下层实现是否排序。
     * 如果有未找到的值，那么会向List中插入空。
     * 
     * @param keys
     * @param keyFilterOrValueFilter 为true使用keyFilter，为false使用valueFilter
     * @return
     * @throws Exception
     */
    List<DuplicateKVPair> mgetWithDuplicateList(List<CloneableRecord> keys, boolean prefixMatch,
                                                boolean keyFilterOrValueFilter) throws TddlException;

    /**
     * 用于判断数据是否已经准备好了。 刚结束查询的时候，这个值是false .直到有数据返回后，这个值会变为true.
     * 并且只要变为true,就不再会回归到false的状态
     * 
     * @return
     */
    public boolean isDone();

    /**
     * 用于输出带缩进的字符串
     * 
     * @param inden
     * @return
     */
    public String toStringWithInden(int inden);

    /**
     * 获取该cursor返回的所有列
     * 
     * @return
     * @throws Exception
     */
    public List<ColumnMeta> getReturnColumns() throws TddlException;
}
