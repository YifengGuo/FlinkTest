package flink_cep.intro_to_cep;

/**
 * Created by guoyifeng on 8/23/18
 */
public class Event {
    int id;
    String eventName;
    double value;

    public Event(int id, String eventName, double value) {
        this.id = id;
        this.eventName = eventName;
        this.value = value;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int getId() {
        return id;
    }

    public String getEventName() {
        return eventName;
    }

    public double getValue() {
        return value;
    }

    public String toString() {
        return this.getId() + " " + this.getEventName();
    }
}
