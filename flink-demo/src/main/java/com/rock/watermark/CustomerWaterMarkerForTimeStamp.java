package com.rock.watermark;

import com.rock.util.MathUtil;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Custom watermark --- for eventtime
 * Date: 2017/12/28
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CustomerWaterMarkerForTimeStamp extends AbsCustomerWaterMarker<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForTimeStamp.class);

    private static final long serialVersionUID = 1L;

    private int pos;

    private long lastTime = 0;

    private TimeZone timezone;

    public CustomerWaterMarkerForTimeStamp(Time maxOutOfOrderness, int pos,String timezone) {
        super(maxOutOfOrderness);
        this.pos = pos;
        this.timezone= TimeZone.getTimeZone(timezone);
    }

    @Override
    public long extractTimestamp(Row row) {
        try {
            Timestamp time = (Timestamp) row.getField(pos);

            long extractTime=time.getTime();

            lastTime = extractTime + timezone.getOffset(extractTime);

            eventDelayGauge.setDelayTime(MathUtil.getIntegerVal((System.currentTimeMillis() - extractTime)/1000));

            return lastTime;
        } catch (RuntimeException e) {
            logger.error("", e);
        }
        return lastTime;
    }

}
