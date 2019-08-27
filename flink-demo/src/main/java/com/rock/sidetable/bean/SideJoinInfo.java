package com.rock.sidetable.bean;

import com.rock.sidetable.cache.AbstractCache;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class SideJoinInfo {
    private String tableName;

    /**
     * 配置参数
     */
    private Map<String, Object> properties;

    /**
     * 返回列名
     */
    private List<String> outCols;

    /**
     * 返回列类型
     */
    private RowTypeInfo rowTypeInfo;

    private List<String> joinCol;

    private List<Integer> joinColIndex;

    private AbstractCache cache;

    public List<String> getOutCols() {
        return outCols;
    }

    public void setOutCols(List<String> outCols) {
        this.outCols = outCols;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public List<String> getJoinCol() {
        return joinCol;
    }

    public void setJoinCol(List<String> joinCol) {
        this.joinCol = joinCol;
    }

    public List<Integer> getJoinColIndex() {
        return joinColIndex;
    }

    public void setJoinColIndex(List<Integer> joinColIndex) {
        this.joinColIndex = joinColIndex;
    }

    public AbstractCache getCache() {
        return cache;
    }

    public void setCache(AbstractCache cache) {
        this.cache = cache;
    }

    public Object getFromProperties(String key) {
        return properties.get(key);
    }
}
