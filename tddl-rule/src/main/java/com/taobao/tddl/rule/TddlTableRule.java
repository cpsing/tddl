package com.taobao.tddl.rule;

import java.util.List;

import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.rule.exceptions.RouteCompareDiffException;
import com.taobao.tddl.rule.model.MatcherResult;
import com.taobao.tddl.rule.model.sqljep.ComparativeMapChoicer;

/**
 * 基于tddl管理体系的table rule实现
 * 
 * @author jianghang 2013-11-5 上午11:04:35
 * @since 5.0.0
 */
public interface TddlTableRule extends Lifecycle {

    /**
     * 根据当前规则，计算table rule结果. <br/>
     * ps. 当前规则可能为local本地规则或者是当前使用中的远程规则
     * 
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @return
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args);

    /**
     * 指定version规则版本，计算table rule结果
     * 
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param specifyVtr 指定规则
     * @return
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args, String version);

    /**
     * 指定规则，计算table rule结果
     * 
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param specifyVtr 指定规则
     * @return
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRoot specifyVtr);

    /**
     * 根据当前规则，同时支持新旧规则模式的计算 <br/>
     * ps. 运行时切库会同时指定新旧两个版本号，针对读请求使用老规则，针对写请求如果新老规则相同则正常返回，如果不同则抛diff异常.<br/>
     * 
     * <pre>
     * 一般切换步骤：
     * 1. 根据新规则，将老库数据做一次数据复制
     * 2. 线上配置新老规则，同时生效
     * 3. 应用前端停写，等待后端增量数据迁移完全追平
     * 4. 线上配置为新规则
     * 5. 删除老库上的数据
     * </pre>
     * 
     * @param sqlType 原始sql类型
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @return
     * @throws RouteCompareDiffException
     */
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args) throws RouteCompareDiffException;

    // ==================== 以下方法可支持非jdbc协议使用rule =================

    /**
     * 根据当前规则，计算table rule结果. <br/>
     * 
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     * @return
     */
    public MatcherResult route(String vtab, String condition);

    /**
     * 指定规则，计算table rule结果. <br/>
     * 
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     * @return
     */
    public MatcherResult route(String vtab, String condition, VirtualTableRoot specifyVtr);

    /**
     * 指定version规则版本，计算table rule结果. <br/>
     * 
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     * @return
     */
    public MatcherResult route(String vtab, String condition, String version);

}
