package flink_process_function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guoyifeng on 8/23/18
 */

/**
 * use case of process function: access to the basic building blocks of all (acyclic) streaming applications
 * so a ProcessFunction could have access to:
 *                              1. events (elements in streaming)
 *                              2. state (fault-tolerant, consistent, only on keyed stream)), by initialize
 *                                 some kind of Keyed State (ValueState, ListState, MapState etc) and get access to it
 *                                 by initialing StateDescriptor in open()
 *                              3. timers (event time and processing time, only on keyed stream)
 *                                 So during process function, it can get timestamp or register time service via
 *                                 runtime context, and complete need like onTimer trigger etc.
 *
 *  The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
 *  It handles events by being invoked for each event received in the input stream(s).
 */

/**
 * the goal is to build a key-count pair and and emits a key/count pair whenever a (customized time) like a
 * minute passes (in event time) without an update for that key
 */
public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {
    // the state maintained by this process function
    // it is a wrapper class to record current element's key, count and its timestamp
    private ValueState<CountWithTimestamp> state;

    private Logger log = LoggerFactory.getLogger(CountWithTimeoutFunction.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("jobState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        // retrieve current State
        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = value.f0;
        }

        // update count
        current.count++;

        // update timestamp
        // NOTE: ctx.timestamp might be null
        current.lastModifiedTime = ctx.timestamp() == null ? 0 : ctx.timestamp();

        // update state
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        // Registers a timer to be fired when the event time watermark passes the given time.
        ctx.timerService().registerEventTimeTimer(current.lastModifiedTime + 60000);
    }

    /**
     * output data if timer is fired
     *
     * @param timestamp the timestamp when registered timer is fired
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        CountWithTimestamp current = state.value();

        // check if current timestamp equals lastModifiedTime + 1mins
        if (timestamp == current.lastModifiedTime + 60000) {
            out.collect(new Tuple2<>(current.key, current.count));
        }
    }
}