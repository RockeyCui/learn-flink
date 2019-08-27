package com.rock.sidetable;

import org.apache.flink.types.Row;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public interface SideTableJoinFunc {

    /**
     * 将单行数据 join 维表数据
     *
     * @param input         源数据
     * @param sideTableData 匹配到的维表数据
     * @return org.apache.flink.types.Row
     * @author cuishilei
     * @date 2019/8/16
     */
    Row joinData(Row input, Object sideTableData);

}
