package com.rock.watermark;

/**
 * @author cuishilei
 * @date 2019/7/31
 */
public class MyEvent {
    private int id;

    private int num;

    private String eventTime;

    public MyEvent() {
    }

    public MyEvent(int id, int num, String eventTime) {
        this.id = id;
        this.num = num;
        this.eventTime = eventTime;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "id=" + id +
                ", num=" + num +
                ", eventTime='" + eventTime + '\'' +
                '}';
    }
}
