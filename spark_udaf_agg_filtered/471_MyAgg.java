//https://raw.githubusercontent.com/kuzetech/lab/201503691d749be2b64f5d4012f9b92aa307c12b/lab-java-all/spark/src/main/java/com/kuzetech/bigdata/spark/funnel/MyAgg.java
package com.kuzetech.bigdata.spark.funnel;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.ArrayList;
import java.util.Comparator;

public class MyAgg extends Aggregator<Event, EventList, Integer> {

    @Override
    public EventList zero() {
        EventList e = new EventList();
        e.setList(new ArrayList<Event>());
        return e;
    }

    @Override
    public EventList reduce(EventList b, Event a) {
        b.getList().add(a);
        return b;
    }

    @Override
    public EventList merge(EventList b1, EventList b2) {
        b1.getList().addAll(b2.getList());
        return b1;
    }

    @Override
    public Integer finish(EventList reduction) {
        reduction.getList().sort(Comparator.comparing(Event::getTime));
        EventsTimestamp[] timestamps = App.findFunnel(reduction.getList(), 17, 7 * 24 * 60 * 60);
        if(timestamps == null) {
            return 0;
        }
        return timestamps.length;
    }

    @Override
    public Encoder<EventList> bufferEncoder() {
        return Encoders.bean(EventList.class);
    }

    @Override
    public Encoder<Integer> outputEncoder() {
        return Encoders.INT();
    }
}
