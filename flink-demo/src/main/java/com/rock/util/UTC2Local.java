package com.rock.util;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class UTC2Local extends ScalarFunction {

    public Timestamp eval(Timestamp s) {

        return new Timestamp(s.getTime() + 28800000);
    }

}
