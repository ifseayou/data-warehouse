package com.isea.imall.dw.fi;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {
    public static final String FI_HEADER_TYPE = "logType";
    public static final String FI_HEADER_TYPE_STARTUP = "startup";
    public static final String FI_HEADER_TYPE_EVENT = "event";
    Gson gson = null;

    @Override
    public void initialize() {
        gson = new Gson();
    }

    @Override
    public Event intercept(Event event) {
        String logString = new String(event.getBody());
        HashMap logMap = gson.fromJson(logString, HashMap.class);
        String type = (String) logMap.get("type");
        Map<String, String> headers = event.getHeaders();
        if (FI_HEADER_TYPE_STARTUP.equals(type)) {
            headers.put(FI_HEADER_TYPE, FI_HEADER_TYPE_STARTUP);
        } else {
            headers.put(FI_HEADER_TYPE, FI_HEADER_TYPE_EVENT);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    /**
     * 该静态内部类的作用就是用来创建自定义对象，提供给flume使用，
     */

    public static class Builder implements Interceptor.Builder {

        /**
         * @return 返回创建的自定义的拦截器对象
         */
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
            // 可以通过context得到flume.conf 中设置的参数，传递给Interceptor
        }
    }
}
