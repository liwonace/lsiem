import java.io.*;
import java.lang.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;
import org.apache.log4j.BasicConfigurator;
import java.text.ParseException;

/* .bashrc

 export KAFKA_LIB='/opt/kafka_2.9.1-0.8.2.1/libs'

 export CLASSPATH=.:$KAFKA_LIB/jopt-simple-3.2.jar:$KAFKA_LIB/kafka-clients-0.8.2.1.jar

                   :$KAFKA_LIB/kafka_2.9.1-0.8.2.1.jar:$KAFKA_LIB/log4j-1.2.16.jar

                   :$KAFKA_LIB/metrics-core-2.2.0.jar:$KAFKA_LIB/scala-library-2.9.1.jar

                   :$KAFKA_LIB/slf4j-api-1.7.6.jar:$KAFKA_LIB/slf4j-log4j12-1.6.1.jar

                   :$KAFKA_LIB/snappy-java-1.1.1.6.jar:$KAFKA_LIB/zkclient-0.3.jar

                   :$KAFKA_LIB/zookeeper-3.4.6.jar
*/

class message_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String read_path = "/var/log/messages";
		String write_path = "/home/kafka_test/logs/";
		String write_name = "cent7-messages.log";
		String topic = "cent7-messages";
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
		String read_path = "/var/log/secure";
		String write_path = "/home/kafka_test/logs/";
		String write_name = "cent7-secure.log";
		String topic = "cent7-secure";
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
		String read_path = "/var/log/cron";
		String write_path = "/home/kafka_test/logs/";
		String write_name = "cent7-cron.log";
		String topic = "cent7-cron";
		try{
			while(true){
				css.syslog(read_path,write_name,write_path,topic);
				Thread.sleep(10*1000);
			}
		}catch(InterruptedException e){
		}
	}
}

public class Cent7_Producer {

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
	public void syslog(String read_path, String write_name, String write_path, String topic){
		try{
			//BasicConfigurator.configure();
			String last_line = null;
			String s = null;
			String path = write_path;
			String file_name = write_name;
			File w_path = new File(path);
			File w_file = new File(path,file_name);
			String line_read = null;
			int check = 0;
			int count = 0;			

			if(!w_path.exists()) {
				w_path.mkdirs();
			}
			if(!w_file.exists()) {
				w_file.createNewFile();
			}
			
			Properties configs = new Properties();

			configs.put("bootstrap.servers", "192.168.7.70:9092");
			configs.put("acks", "all");
			configs.put("block.on.buffer.full", "true");
			configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
			
			BufferedReader line_conf = new BufferedReader(new FileReader(w_file));
			while ((s = line_conf.readLine()) != null) {
				line_read = s;
				s = null;
			}
			line_conf.close();
			BufferedReader conf = new BufferedReader(new FileReader(read_path));
			while ((s = conf.readLine()) != null) {				
				if ((s.length() > 0 && check == 1) || line_read == null) {
					producer.send(new ProducerRecord<>(topic, s),
					(metadata, exception) -> {
						if (metadata != null) {
							System.out.println(
								"partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");
						} else {
							exception.printStackTrace();
						}
					});
					last_line = s;					
					producer.flush();
				}
				if(s.equals(line_read)){
					check = 1;
				}
				count++;
				s = null;
			}

			BufferedWriter out = new BufferedWriter(new FileWriter(w_file));
			if(last_line != null && last_line.length() != 0){
				out.write(last_line);
			}else if(count > 0 && check == 0 && line_read != null){
				out.write("");
			}else{
				out.write(line_read);
			}

			last_line = null;
			line_read = null;
			read_path = null;
			write_name = null;
			write_path = null;
			topic = null;
			path = null;
			w_file = null;
			w_path = null;
			file_name = null;

			out.close();
			conf.close();		
			producer.close();
		}catch (IOException e) {
    	        
    	}catch (NullPointerException e) {
    	        
    	}
	}
}
