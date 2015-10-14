/*
 * 文件名：MainTest.java
 * 描述： MainTest.java
 * 修改人：zhengmo
 * 修改时间：2015年10月14日
 * 修改内容：新增
 */
package com.zhengmo.data.transaction.test;

import javax.sql.DataSource;

import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;

import com.zhengmo.data.transaction.TransactionCallBack;
import com.zhengmo.data.transaction.util.ChainedTransactionManagerUtil;

/**
 * @author zhengmo
 */
public class MainTest {
    public static void main(String[] args) {
        final SqlSessionFactoryBean bean = null;
        final ChainedTransactionManagerUtil util = new ChainedTransactionManagerUtil();
        DataSource ds1 = null;
        util.putDataSource("key1", ds1);
        DataSource ds2 = null;
        util.putDataSource("key2", ds2);
        Integer code = util.doTransaction(new TransactionCallBack<Integer>() {
            @Override
            public Integer doTransaction() throws Exception {
                SqlSessionTemplate sql = util.genSqlSessionTemplate(bean, "key1");
                int count = sql.update("update a set b=1 where c=2 ");
                sql = util.genSqlSessionTemplate(bean, "key2");
                count += sql.update("update a set b=1 where c=2 ");
                return count;
            }
        });
        if (code > 0) {
            System.out.println("ok");
        }
    }
}
