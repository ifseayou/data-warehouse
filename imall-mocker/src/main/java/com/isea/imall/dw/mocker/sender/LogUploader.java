package com.isea.imall.dw.mocker.sender;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * 将我们随机出来的日志文件上传到我们的日志服务器
 * 需要指定日志服务器的地址，该服务器来负责接收我们上传到的日志。
 */
public class LogUploader {
    /**
     * 将log日志信息上传到日志服务器
     *
     * @param log 日志信息
     */
    public static void sendLogStream(String log) {
        try {
            // 指定日志服务器的URL地址
//            URL url = new URL("http://toLinuxLogServer/isea_log");
            URL url = new URL("http://toLocalLogServer/isea_log");

            // 使用服务器的URL来获取一个连接
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            // 设置向日志服务器的请求方式
            conn.setRequestMethod("POST");

            // 时间头用来提供Server进行适中校对
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");

            // 允许上传数据
            conn.setDoOutput(true);

            // 设置请求的头信息，设置内容类型是JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            // 输出流，上传即为写
            OutputStream out = conn.getOutputStream();
            out.write(("log=" + log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
