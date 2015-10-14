/*
 * 文件名：ChainedTransactionManagerUtil.java
 * 描述： ChainedTransactionManagerUtil.java
 * 修改人：zhengmo
 * 修改时间：2015年7月7日
 * 修改内容：新增
 */
package com.zhengmo.data.transaction.util;

import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.zhengmo.data.transaction.ChainedTransactionManager;
import com.zhengmo.data.transaction.TransactionCallBack;

/**
 * 链式事务工具类.
 * 
 * <pre>
 * ##############使用方法
 * 1.final ChainedTransactionManagerUtil tx = new ChainedTransactionManagerUtil();
 * 2.tx.putDataSource(key,ds);
 * 3.tx.doTransaction(new TransactionCallBack(){
 *      tx.genSqlSessionTemplate(key).selectList(args);
 * });
 * ##############使用方法
 * </pre>
 * @author zhengmo
 */
public class ChainedTransactionManagerUtil {
    /**
     * @事务是否开始
     */
    private boolean isBegin = false;
    /**
     * @sqlSessionTemplate缓存
     */
    private final Map<String, SqlSessionTemplate> sqlSessionTemplateCacheMap = new ConcurrentHashMap<String, SqlSessionTemplate>();
    /**
     * @数据源缓存
     */
    private final Map<String, Object> dataSourceCacheMap = new ConcurrentHashMap<String, Object>();
    /**
     * @事务管理器缓存
     */
    private final Map<String, Object> txCacheMap = new ConcurrentHashMap<String, Object>();
    /**
     * @临时工厂
     */
    private final SqlSessionFactoryBean tmpFactory = new SqlSessionFactoryBean();
    /**
     * @数据源集合Key
     */
    private static final String DATASOURCE_LIST = "_dataSourceList_";
    /**
     * @事务管理器集合Key
     */
    private static final String DATASOURCE_TX_LIST = "_dataSourceTxList_";
    /**
     * 构造函数.
     */
    public ChainedTransactionManagerUtil() {
    }
    /**
     * 事务开始.
     * @param <T> t 返回结果
     * @param callBack 回调函数
     * @return T t
     */
    public <T> T doTransaction(TransactionCallBack<T> callBack) {
        List<PlatformTransactionManager> dstx = getTxList();
        if (dstx == null || dstx.size() == 0) {
            throw new RuntimeException("未设置数据源或事务管理器未初始化");
        }
        T result = null;
        ChainedTransactionManager chainedTx = new ChainedTransactionManager(dstx.toArray(new PlatformTransactionManager[dstx.size()]));
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        def.setIsolationLevel(TransactionDefinition.ISOLATION_DEFAULT);
        TransactionStatus status = chainedTx.getTransaction(def);
        try {
            isBegin = true;
            result = callBack.doTransaction();
            chainedTx.commit(status);
        } catch (RuntimeException ex) {
            rollbackOnException(chainedTx, status, ex);
            ex.printStackTrace();
            throw ex;
        } catch (Error err) {
            rollbackOnException(chainedTx, status, err);
            err.printStackTrace();
            throw err;
        } catch (Exception ex) {
            rollbackOnException(chainedTx, status, ex);
            ex.printStackTrace();
            throw new UndeclaredThrowableException(ex, "TransactionCallback threw undeclared checked exception");
        } catch (Throwable e) {
            rollbackOnException(chainedTx, status, e);
            e.printStackTrace();
            throw e;
        }
        return result;
    }
    /**
     * 根据时间按年生成key.
     * @param date 时间
     * @param otherKey hashKey,spKey
     * @return key eg.2015_hash_sp
     */
    public String genMapKey(Date date, String... otherKey) {
        StringBuffer sb = new StringBuffer();
        sb.append(new SimpleDateFormat("yyyy").format(date));
        if (otherKey != null && otherKey.length > 0) {
            for (String key : otherKey) {
                sb.append("_");
                sb.append(key);
            }
        }
        return sb.toString();
    }
    /**
     * 生成SqlSessionTemplate. 如果缓存key已在，则返回缓存数据
     * 
     * @param orignalSqlSessionFactory 原始的SqlSessionFactoryBean 反射调用获取configLocation
     * @param key 如有不为NULL，表示缓存的值
     * @return SqlSessionTemplate
     */
    public SqlSessionTemplate genSqlSessionTemplate(SqlSessionFactoryBean orignalSqlSessionFactory, String key) {
        if (!dataSourceCacheMap.containsKey(key)) {
            throw new RuntimeException("该key所在的数据源不在链式事务上");
        }
        checkMapKey(key);
        SqlSessionTemplate sqlSessionTemplate = (SqlSessionTemplate) sqlSessionTemplateCacheMap.get(key);
        if (sqlSessionTemplate != null) {
            return sqlSessionTemplate;
        }
        try {
            if (orignalSqlSessionFactory == null) {
                throw new RuntimeException("orignalSqlSessionFactory 不能为NULL");
            }
            DataSource ds = (DataSource) dataSourceCacheMap.get(key);
            if (ds == null) {
                throw new RuntimeException("该key所在的数据源为NULL");
            }
            Field field = orignalSqlSessionFactory.getClass().getDeclaredField("configLocation");
            field.setAccessible(true);
            Object configLocation = field.get(orignalSqlSessionFactory);
            tmpFactory.setConfigLocation((org.springframework.core.io.Resource) configLocation);
            tmpFactory.setDatabaseIdProvider(new DatabaseIdProvider() {
                @Override
                public void setProperties(Properties arg0) {
                }
                @Override
                public String getDatabaseId(DataSource arg0) throws SQLException {
                    return null;
                }
            });
            tmpFactory.setDataSource(ds);
            tmpFactory.afterPropertiesSet();
            sqlSessionTemplate = new SqlSessionTemplate(tmpFactory.getObject());
            if (sqlSessionTemplate != null && key != null) {
                sqlSessionTemplateCacheMap.put(key, sqlSessionTemplate);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return sqlSessionTemplate;
    }
    /**
     * @获取数据源列表
     * @return 返回List
     */
    @SuppressWarnings({"unchecked"})
    public List<DataSource> getDataSources() {
        List<DataSource> list = new ArrayList<DataSource>();
        if (dataSourceCacheMap == null || dataSourceCacheMap.isEmpty()) {
            return list;
        }
        for (String key : dataSourceCacheMap.keySet()) {
            Object obj = dataSourceCacheMap.get(key);
            if (obj instanceof List) {
                list.addAll((List<DataSource>) obj);
            } else if (obj instanceof DataSource) {
                list.add((DataSource) obj);
            }
        }
        return list;
    }
    /**
     * 获取缓存的sqlSessionTemplate.
     * @param key key
     * @return sqlSessionTemplate
     */
    public SqlSessionTemplate getSqlSessionTemplate(String key) {
        if (!DATASOURCE_LIST.equals(key) && !DATASOURCE_TX_LIST.equals(key)) {
            return (SqlSessionTemplate) sqlSessionTemplateCacheMap.get(key);
        } else {
            throw new IllegalArgumentException("key 不能为:" + DATASOURCE_LIST + " 或 " + DATASOURCE_TX_LIST);
        }
    }
    /**
     * @获取事务管理器列表
     * @return 返回List
     */
    @SuppressWarnings("unchecked")
    public List<PlatformTransactionManager> getTxList() {
        List<PlatformTransactionManager> list = new ArrayList<PlatformTransactionManager>();
        if (txCacheMap == null || txCacheMap.isEmpty()) {
            return list;
        }
        for (String key : txCacheMap.keySet()) {
            Object obj = txCacheMap.get(key);
            if (obj instanceof List) {
                list.addAll((List<PlatformTransactionManager>) obj);
            } else if (obj instanceof PlatformTransactionManager) {
                list.add((PlatformTransactionManager) obj);
            }
        }
        return list;
    }
    /**
     * 
     * 添加数据源.
     * @param key key
     * @param ds 数据源
     */
    public void putDataSource(String key, DataSource ds) {
        if (isBegin) {
            throw new RuntimeException("事务已经开始，不能添加数据源");
        }
        if (ds == null || key == null) {
            throw new RuntimeException("key 或 ds 不能为空");
        }
        checkMapKey(key);
        dataSourceCacheMap.put(key, ds);
        DataSourceTransactionManager tx = new DataSourceTransactionManager(ds);
        txCacheMap.put(key, tx);
    }
    /**
     * 缓存sqlSessionTemplate.
     * @param key key
     * @param sqlSessionTemplate sqlSessionTemplate
     */
    public void putSqlSessionTemplate(String key, SqlSessionTemplate sqlSessionTemplate) {
        if (!DATASOURCE_LIST.equals(key) && !DATASOURCE_TX_LIST.equals(key)) {
            sqlSessionTemplateCacheMap.put(key, sqlSessionTemplate);
        } else {
            throw new IllegalArgumentException("key 不能为:" + DATASOURCE_LIST + " 或 " + DATASOURCE_TX_LIST);
        }
    }
    /**
     * 检测MAP key.
     * @param key .
     */
    private void checkMapKey(String key) {
        if (DATASOURCE_LIST.equals(key) || DATASOURCE_TX_LIST.equals(key)) {
            throw new IllegalArgumentException("key 不能为:" + DATASOURCE_LIST + " 或 " + DATASOURCE_TX_LIST);
        }
    }
    /**
     * 异常回滚.
     * @param chainedTx 事务管理器
     * @param status 事务状态
     * @param ex 异常
     * @throws TransactionException 异常
     */
    private void rollbackOnException(PlatformTransactionManager chainedTx, TransactionStatus status, Throwable ex) throws TransactionException {
        try {
            status.setRollbackOnly();
            chainedTx.rollback(status);
        } catch (TransactionSystemException ex2) {
            ex2.initApplicationException(ex);
            throw ex2;
        } catch (Exception ex2) {
            throw ex2;
        } catch (Error err) {
            throw err;
        }
    }
}
