package flink_windows;

import java.util.Date;

/**
 * Created by guoyifeng on 9/11/18
 */
public class FlinkTestEvent {
    public String key;
    public long value;
    public long lastModifiedTime;

    public FlinkTestEvent() {
        this.key = "";
        this.value = 0L;
        this.lastModifiedTime = 0L;
    }

    public FlinkTestEvent(String key, long value, long lastModifiedTime) {
        this.key = key;
        this.value = value;
        this.lastModifiedTime = lastModifiedTime;
    }

    @Override
    public String toString() {
        long time = this.lastModifiedTime;
        return "Key: " + this.key + " Value: " + this.value + " Timestamp: " + (time > 50000 ?  new Date(time) : time);
    }
}
