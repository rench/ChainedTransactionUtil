/*
 * 文件名：TransactionCallBack.java
 * 描述： TransactionCallBack.java
 * 修改人：zhengmo
 * 修改时间：2015年7月7日
 * 修改内容：新增
 */
package com.zhengmo.data.transaction;
/**
 * 回调函数接口.
 * @author zhengmo
 * @param <T> t
 */
public interface TransactionCallBack<T> {
    /**
     * 
     * 回调函数.
     * @return T t
     * @throws Exception 异常
     */
    public T doTransaction() throws Exception;
}
