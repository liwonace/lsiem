import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.BufferedInputStream;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;


public class Sql_producer{

    public static void main(String[] args){

        ServerSocket serversocket = null;        
        Socket socket = null;        
        InputStream in = null;   
        int read = 0;
        BufferedInputStream bin = null;
        byte[] stringbuf = new byte[100000];
        String rtemp = null;
        String read_data[];
        String permission_ip[] = {"192.168.7.16"}; 
        
        try{
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
                    for(int i=0;i < permission_ip.length;i++){
                        if(permission_ip[i].equals(read_data[2])){
                            System.out.println("Permission OK");
                            permission_check = 1;
                        }
                    }
                    System.out.println("packet : " + rtemp+", len : "+rtemp.length());
                    if(permission_check == 0){
                        kcls.syslog(rtemp,"sql-data");
                    }
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
            configs.put("bootstrap.servers", "192.168.7.70:9092");
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
