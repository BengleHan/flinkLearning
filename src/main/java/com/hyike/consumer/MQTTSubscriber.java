package com.hyike.consumer;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/****
 ** @author: langel
 ** @date: 15:29 2020/11/17
 ** @system: project
 ** @version: 1.0
 ** @functional:
 **
 **
 ****/
public class MQTTSubscriber implements Runnable{
    //    private final static String SERVER_MONITOR_CLIENT_ID = "SERVER_MONITOR_CLIENT_ID"+System.currentTimeMillis();
    //日志服务
//    private static Logger logger = LoggerFactory.getLogger(MQTTSubscriber.class);
    /*MQTT连接对象*/
    protected MqttClient mqtt;

    private MqttConnectOptions options;

    private static final String clientid = "tdata_client_id";
    private static  String str = "";

//    public MQTTSubscriber(com.hyike.mq.consumer.MqttCallbackHandle callBack) {
//        this.callBack = callBack;
//    }

    @Override
    public void run() {
        try {
            lunch();
        } catch (Exception exception) {
//            logger.error(exception.getMessage(),exception);
        }
    }

    private void lunch() throws Exception {
        System.out.println("开始启动MQTT协议监听");
        if (mqtt != null) {
            throw new Exception("监听已启动，请勿重新启动.");
        }

        /*初始化MQTT服务连接对象*/
        mqtt = new MqttClient("tcp://8.133.160.105:61613",clientid,new MemoryPersistence());
        // 设置是否清空session,这里如果设置为false表示服务器会保留客户端的连接记录，设置为true表示每次连接到服务器都以新的身份连接
        options = new MqttConnectOptions();
        options.setCleanSession(false);
        // 设置连接的用户名
        options.setUserName("admin");
        // 设置连接的密码
        options.setPassword("password".toCharArray());
        // 设置超时时间 单位为秒
        options.setConnectionTimeout(10);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送个消息判断客户端是否在线，但这个方法并没有重连的机制
        options.setKeepAliveInterval(20);
        // 设置回调
        mqtt.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable throwable) {

            }

            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                System.out.println(" 开始处理:");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

            }
        });
        System.out.println(" 开始订阅:");
        int[] Qos  = {0};
        String[] topic1 = {"iotData-e"};
        mqtt.subscribe(topic1, Qos);
    }


    public static void main(String[] args) throws MqttException {
        MQTTSubscriber client = new MQTTSubscriber();
        Thread thread = new Thread(client);
        thread.start();
    }

}
