import java.io.*;
import java.lang.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;

/* 
 ------import library------ 
 
 jopt-simple-3.2.jar
 kafka-clients-0.8.2.1.jar
 kafka_2.9.1-0.8.2.1.jar
 log4j-1.2.16.jar
 metrics-core-2.2.0.jar
 scala-library-2.9.1.jar
 slf4j-api-1.7.6.jar
 slf4j-log4j12-1.6.1.jar
 snappy-java-1.1.1.6.jar
 zkclient-0.3.jar
 zookeeper-3.4.6.jar
 -------------------------
*/
class message_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\Application.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-application.txt";
		String topic = "win10-application";
		try{
			while(true){
				css.syslog(read_path,write_name,write_path,topic);
				Thread.sleep(10*1000);
			}
		}catch(InterruptedException e){
		}
	}
}

class secure_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\System.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-system.txt";
		String topic = "win10-system";
		try{
			while(true){
				css.syslog(read_path,write_name,write_path,topic);
				Thread.sleep(10*1000);
			}
		}catch(InterruptedException e){
		}
	}
}

class cron_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\Security.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-security.txt";
		String topic = "win10-security";
		try{
			while(true){
				css.syslog(read_path,write_name,write_path,topic);
				Thread.sleep(10*1000);
			}
		}catch(InterruptedException e){
		}
	}
}

public class Win_Producer {

	public static void main(String[] args) throws IOException {
		Thread messages = new message_thread();
		Thread secure = new secure_thread();
		Thread cron = new cron_thread();

		messages.start();
		secure.start();
		cron.start();
	}
}

class send_kafka{
	int err_check = 0;
	public void syslog(String read_path, String write_name, String write_path, String topic){
		try{
			String last_line = null;
			String s = null;
			String path = write_path;
			String file_name = write_name;
			File w_file = new File(path,file_name);
			String line_read = null;
			int check = 0;			
			String send_str = null;
			String before_s = " ";
									
			Properties configs = new Properties();
			configs.put("bootstrap.servers", "192.168.7.70:9093");
			configs.put("acks", "all");
			configs.put("block.on.buffer.full", "true");
			configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
			BufferedReader line_conf = new BufferedReader(new FileReader(w_file));
			
			while ((s = line_conf.readLine()) != null) {
				line_read = s;				
			}
			line_conf.close();
			BufferedReader conf = new BufferedReader(new FileReader(read_path));
			while ((s = conf.readLine()) != null && before_s != null) {				
				if(s.contains("Event[")) {					
					if (send_str != null && check == 1) {
						System.out.println(send_str);
						producer.send(new ProducerRecord<>(topic, send_str),
						(metadata, exception) -> {
							if (metadata != null) {
								System.out.println(
									"partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");								
							} else {
								exception.printStackTrace();
								err_check = 1;
							}
						});
						if(err_check == 0) {
							last_line = send_str;
						}						
						producer.flush();
					}
					
					if(send_str != null && send_str.equals(line_read) || line_read == null){
						check = 1;
					}
					
					send_str = s;
				}else {
					send_str += s;
				}
				before_s = s;				
			}
			BufferedWriter out = new BufferedWriter(new FileWriter(w_file));
			if(last_line != null && last_line.length() != 0){
				out.write(last_line);			
			}else{
				out.write(line_read);
			}

			out.close();
			conf.close();		
			producer.close();
		}catch (IOException e) {
    	        
    	}catch (NullPointerException e) {
    	        
    	}
	}
}