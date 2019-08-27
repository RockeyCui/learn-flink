package com.rock.watermark;

import com.rock.util.MathUtil;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

public class CustomerWaterMarkerForLong extends AbsCustomerWaterMarker<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForLong.class);

    private static final long serialVersionUID = 1L;

    private int pos;

    private long lastTime = 0;

    private TimeZone timezone;

    public CustomerWaterMarkerForLong(Time maxOutOfOrderness, int pos, String timezone) {
        super(maxOutOfOrderness);
        this.pos = pos;
        this.timezone = TimeZone.getTimeZone(timezone);
    }

    @Override
    public long extractTimestamp(Row row) {

        try {
            Long extractTime = MathUtil.getLongVal(row.getField(pos));

            lastTime = extractTime + timezone.getOffset(extractTime);

            eventDelayGauge.setDelayTime(MathUtil.getIntegerVal((System.currentTimeMillis() - extractTime) / 1000));

            return lastTime;
        } catch (Exception e) {
            logger.error("", e);
        }
        return lastTime;
    }

}
