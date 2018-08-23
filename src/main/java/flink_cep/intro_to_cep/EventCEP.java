package flink_cep.intro_to_cep;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

/**
 * Created by guoyifeng on 8/23/18
 */
public class EventCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // the result will differ if set to ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Event> inputStream = env.fromElements(
                Tuple2.of(new Event(1, "start", 1.0), 5L),
                Tuple2.of(new Event(2, "middle", 2.0), 1L),
                Tuple2.of(new Event(3, "end", 3.0), 3L),
                Tuple2.of(new Event(4, "end", 4.0), 10L), // trigger 2，3，1
                Tuple2.of(new Event(5, "middle", 5.0), 7L),
                // last element for high final watermark
                Tuple2.of(new Event(6, "middle", 6.0), 100L) // trigger 5，4
        ).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Event, Long>>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Tuple2<Event, Long> lastElement, long extractedTimestamp) {
                return new Watermark(lastElement.f1 - 5);  // watermark setting
            }

            @Override
            public long extractTimestamp(Tuple2<Event, Long> element, long previousElementTimestamp) {
                return element.f1;
            }
        }).map(tuple2 -> tuple2.f0);

        // define patterns  --> when data in stream matches defined patterns, do something
        // here is to find a sequence of events which has "start", "middle" and "end"

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            public boolean filter(Event event) throws Exception {
                return event.getEventName().equals("start");
            }
        }).followedBy("middle").where(new SimpleCondition<Event>() {
            public boolean filter(Event event) throws Exception {
                return event.getEventName().equals("middle");
            }
        }).followedBy("end").where(new SimpleCondition<Event>() {
            public boolean filter(Event event) throws Exception {
                return event.getEventName().equals("end");
            }
        });

        // execute CEP
        // print each event's id in a complete sequence
        DataStream<String> output = CEP.pattern(inputStream, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> map) throws Exception { // each pattern's match result stored in a list
                StringBuilder sb = new StringBuilder();
                map.forEach((k, v) -> {
                    for (Event e : v) {  // 3 patterns so there are 3 lists
                        System.out.print(e);
                    }
                    System.out.println();
                });
                sb.append(map.get("start").get(0).getId() + " ");
                sb.append(map.get("middle").get(0).getId() + " ");
                sb.append(map.get("end").get(0).getId() + " ");
                return sb.toString();
            }
        });

        output.print();  // 1 5 4
        // The reason the result is 1 5 4 is that env's time is event time
        // So event in the stream will not be processed directly and update NFA
        // instead, the element will be firstly cached in a Queue, and until
        // onEventTime trigger and new WaterMark arrives, event in the queue will be
        // processed by its timestamp. Elements whose timestamp >= watermark will be processed in particular
        // In this way, the data in result stream is guaranteed in-order

        output.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                File f = new File("src/main/java/flink_cep/intro_to_cep/cep_output.txt");
                FileWriter fw = new FileWriter(f);
                fw.write(value);
                fw.close();
            }
        });
        env.execute("cep job");
    }
}
