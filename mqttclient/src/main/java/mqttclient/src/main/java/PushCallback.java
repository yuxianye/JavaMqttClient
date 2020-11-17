package mqttclient.src.main.java;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 发布消息的回调类
 *
 * 必须实现MqttCallback的接口并实现对应的相关接口方法CallBack 类将实现 MqttCallBack。
 * 每个客户机标识都需要一个回调实例。在此示例中，构造函数传递客户机标识以另存为实例数据。
 * 在回调中，将它用来标识已经启动了该回调的哪个实例。
 * 必须在回调类中实现三个方法：
 *
 *  public void messageArrived(MqttTopic topic, MqttMessage message)接收已经预订的发布。
 *
 *  public void connectionLost(Throwable cause)在断开连接时调用。
 *
 *  public void deliveryComplete(MqttDeliveryToken token))
 *  接收到已经发布的 QoS 1 或 QoS 2 消息的传递令牌时调用。
 *  由 MqttClient.connect 激活此回调。
 *
 */
public class PushCallback implements MqttCallback {

    public void connectionLost(Throwable cause) {

        // 连接丢失后，一般在这里面进行重连
        System.out.println("连接断开，可以做重连");
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("deliveryComplete---------" + token.isComplete());
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        // subscribe后得到的消息会执行到这里面
        System.out.println("接收消息主题 : " + topic);
        System.out.println("接收消息Qos : " + message.getQos());
        System.out.println("接收消息内容 : " + new String(message.getPayload()));

        Connection conn = null;
        PreparedStatement ps = null;

        try{

            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            Date  date=new Date();
            String str = format.format(new Date());
            String name= str +"_"+topic.replace('/','_')+"_"+message.getQos();
            System.out.println(name);

            OutputStream outputStream=new FileOutputStream( name);
            File file =new File(name);
            if(!file.exists()){
                file.createNewFile();
            }
            outputStream.write(message.getPayload());
            outputStream.close();
            System.out.println(str+"写入完成，数据长度"+message.getPayload().length);


//存入数据库

//                conn = DBConn.getConnection();
//                String sql = "INSERT INTO mqtt_msg (msgid,topic,node,qos,retain,payload,arrived) VALUES(?,?,?,?,?,?,?)";
//                ps = conn.prepareStatement(sql);
//                //设置占位符对应的值
//                ps.setString(1, str);
//                ps.setString(2, topic);
//                ps.setString(3, null);
//                ps.setInt(4, message.getQos());
//                ps.setString(5, null);
//                ps.setBytes(6, message.getPayload());
//                //ps.setDate(7, new java.sql.Date(date.getYear(),date.getMonth(),date.getDay(),date.getHours(),date.getSeconds(),date.getMinutes());
//
//
//                ps.setDate(7, new java.sql.Date(date.getYear(),date.getMonth(),date.getDay()));
//
//            int insertCount = ps.executeUpdate();
//                System.out.println((insertCount));


        }catch(IOException e){

            e.printStackTrace();

        }
        catch(Exception e) {
        e.printStackTrace();

        }
        finally {
        if(ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }




    }





}