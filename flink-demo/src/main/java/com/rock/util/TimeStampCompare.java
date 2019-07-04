package com.rock.util;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimeStampCompare extends ScalarFunction {

    public Long eval(Timestamp s1, Timestamp s2) {

        return s1.getTime() - s2.getTime();
    }

}
