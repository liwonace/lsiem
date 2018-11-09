import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.net.ServerSocket;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;


public class Sql_producer{

    public static void main(String[] args){

        ServerSocket serversocket = null;        
        Socket socket = null;        
        InputStream in = null;   
        int read = 0;
        BufferedInputStream bin = null;
        byte[] stringbuf = new byte[100000];
        String rtemp = null;
        String s = null;
        String read_data[];
        List<String> permission_ip = new ArrayList<String>();
        List<String> forbidden_table = new ArrayList<String>();
        List<String> injection = new ArrayList<String>();
        File injection_read = new File("sql_injection.txt");
        File permission_ip_read = new File("permission_ip_list.txt");
        File forbidden_table_read = new File("forbidden_table_list.txt");
        String send_msg = null;
        
        try{
            BufferedReader line_conf = new BufferedReader(new FileReader(permission_ip_read));
            while ((s = line_conf.readLine()) != null) {
                permission_ip.add(s);
                s = null;
            }
            line_conf.close();

            line_conf = new BufferedReader(new FileReader(forbidden_table_read));
            while ((s = line_conf.readLine()) != null) {
                forbidden_table.add(s);
                s = null;
            }
            line_conf.close();

            line_conf = new BufferedReader(new FileReader(injection_read));
            while ((s = line_conf.readLine()) != null) {
                injection.add(s);
                s = null;
            }
            line_conf.close();

            serversocket = new ServerSocket(10470);
            System.out.println("server start");
            send_kafka kcls = new send_kafka();
            
            while(true){
                socket = serversocket.accept();
                System.out.println("Client connect");
                in = socket.getInputStream();
                bin = new BufferedInputStream(in);

                while((read = bin.read(stringbuf)) > 0){
                    rtemp = new String(stringbuf, 0, stringbuf.length);
                    rtemp = rtemp.trim();
                    read_data = rtemp.split("~/");
                    int permission_check = 0;
                    int forbidden_check = 0;
                    int injection_check = 0;

                    for(int i=0;i < permission_ip.size();i++){
                        if(permission_ip.get(i).equals(read_data[2])){
                            System.out.println("Permission OK");
                            permission_check = 1;
                            break;
                        }
                    }
                    for(int i=0;i < forbidden_table.size();i++){
                        if(read_data[1].contains(forbidden_table.get(i))){
                            System.out.println("Forbidden Table");
                            forbidden_check = 1;
                            break;
                        }
                    }
                    for(int i=0;i < injection.size();i++){
                        if(read_data[1].contains(injection.get(i))){
                            System.out.println("Sql injection");
                            injection_check = 1;
                            break;
                        }
                    }
                    System.out.println("packet : " + rtemp+", len : "+rtemp.length());
                    if(permission_check == 0 || forbidden_check == 1 || injection_check == 1){
                        if(permission_check == 0){
                            send_msg = read_data[0]+"~/"+read_data[1]+"~/"+read_data[2]+"~/"+read_data[3]+"~/"+read_data[4]+"~/"+"0";
                        }else{
                            send_msg = read_data[0]+"~/"+read_data[1]+"~/"+read_data[2]+"~/"+read_data[3]+"~/"+read_data[4]+"~/"+"1";
                        }
                        if(forbidden_check == 0){
                            send_msg = send_msg+"~/"+"0";
                        }else{
                            send_msg = send_msg+"~/"+"1";
                        }
                        if(injection_check == 0){
                            send_msg = send_msg+"~/"+"0";
                        }else{
                            send_msg = send_msg+"~/"+"1";
                        }
                        kcls.syslog(send_msg,"sql-data");
                    }
                    send_msg = null;
                    read_data = null;
                    rtemp = null;

			    }
            }
        }catch(Exception e){
        }
    }
}

class send_kafka{
    public void syslog(String msg,String topic){
        try{
            Properties configs = new Properties();
            configs.put("bootstrap.servers", "192.168.7.70:9093");
            configs.put("acks", "all");
            configs.put("block.on.buffer.full", "true");
            configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
            System.out.println(msg.length());
            producer.send(new ProducerRecord<>(topic, msg),(metadata, exception) -> {
            if (metadata != null) {
                System.out.println("partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");
            } else {
                exception.printStackTrace();
            }
            });
            
            producer.flush();
            producer.close();

        }catch (NullPointerException e) {
                
        }
    }
}