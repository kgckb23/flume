package cn.kgc.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InterceptorDemo implements Interceptor {

    private ArrayList<Event> opList;

    public void initialize() {
        opList = new ArrayList<Event>();
    }

    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        final String body = new String(event.getBody());
        if (body.startsWith("hello")){
            headers.put("type","hello");
        }else if (body.startsWith("hi")){
            headers.put("type","hi");
        }else {
            headers.put("type","other");
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        opList.clear();
        for (Event event :
                events) {
            opList.add(intercept(event));
        }
        return opList;
    }

    public void close() {
        opList.clear();
        opList = null;
    }

    public static class a1 implements Interceptor.Builder{

        public Interceptor build() {
            return new InterceptorDemo();
        }

        public void configure(Context context) {

        }
    }
}
