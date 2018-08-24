package flink_process_function;

/**
 * Created by guoyifeng on 8/23/18
 */
public class CountWithTimestamp {
    public String key;
    public long count;
    public long lastModifiedTime;

    public CountWithTimestamp() {
        this.key = "";
        this.count = 0L;
        this.lastModifiedTime = 0L;
    }
}