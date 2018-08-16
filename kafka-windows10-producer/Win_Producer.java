import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;
import java.text.ParseException;



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
class application_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String cmd = "cmd.exe /C wevtutil qe C:\\Windows\\System32\\winevt\\Logs\\Application.evtx /lf /f:text > C:\\Users\\shhong\\Desktop\\to_kafka\\Application.txt";
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\Application.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-application.txt";
		String topic = "win10-application";
		
		while(true){
			css.syslog(read_path,write_name,write_path,topic,cmd);				
		}				
	}
}

class ntfs_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String cmd = "cmd.exe /C wevtutil qe C:\\Windows\\System32\\winevt\\Logs\\Microsoft-Windows-Ntfs%4Operational.evtx /lf /f:text > C:\\Users\\shhong\\Desktop\\to_kafka\\Microsoft-Windows-Ntfs%4Operational.txt";
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\Microsoft-Windows-Ntfs%4Operational.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-ntfs.txt";
		String topic = "win10-ntfs";
		
		while(true){
			css.syslog(read_path,write_name,write_path,topic,cmd);				
		}				
	}
}

class system_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String cmd = "cmd.exe /c wevtutil qe C:\\Windows\\System32\\winevt\\Logs\\System.evtx /lf /f:text > C:\\Users\\shhong\\Desktop\\to_kafka\\System.txt";
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\System.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-system.txt";
		String topic = "win10-system";
		
		while(true){
			css.syslog(read_path,write_name,write_path,topic,cmd);
		}		
	}
}

class security_thread extends Thread{
	public void run() {
		send_kafka css = new send_kafka();
		String cmd = "cmd.exe /c wevtutil qe C:\\Windows\\System32\\winevt\\Logs\\Security.evtx /lf /f:text > C:\\Users\\shhong\\Desktop\\to_kafka\\Security.txt";
		String read_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\Security.txt";
		String write_path = "C:\\Users\\shhong\\Desktop\\to_kafka\\logs";
		String write_name = "win10-security.txt";
		String topic = "win10-security";
		
		while(true){
			css.syslog(read_path,write_name,write_path,topic,cmd);
		}		
	}
}

public class Win_Producer {

	public static void main(String[] args) throws IOException {
		Thread application = new application_thread();
		Thread security = new security_thread();
		Thread system = new system_thread();
		Thread ntfs = new ntfs_thread();

		application.start();
		security.start();
		system.start();
		ntfs.start();
	}
}

class send_kafka{
	int err_check = 0;
	public void syslog(String read_path, String write_name, String write_path, String topic,String cmd_evtx){
		try{
			String last_line = null;
			String s = null;
			String path = write_path;
			String file_name = write_name;
			File w_path = new File(path);
			File w_file = new File(path,file_name);
			String line_read = null;
			int check = 0;			
			String send_str = null;
			String before_s = " ";
			int count = 0;			
			
			if(!w_path.exists()) {
				w_path.mkdirs();
			}
			if(!w_file.exists()) {
				w_file.createNewFile();
			}
			Cmd cmd = new Cmd();
			String result = cmd.execCommand(cmd_evtx);
			Thread.sleep(10*1000);
			
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
					if ((send_str != null && check == 1) || (send_str!= null && line_read == null)) {
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
					if(send_str != null && send_str.trim().equals(line_read)){
						check = 1;
					}					
					send_str = s;					
				}else {
					send_str += s;					
				}
				before_s = s;
				count++;
			}
			
			BufferedWriter out = new BufferedWriter(new FileWriter(w_file));
			if(last_line != null && last_line.length() != 0){
				out.write(last_line.trim());
			}else if(count > 0 && check == 0 && line_read != null) {
				out.write("");
			}else{
				out.write(line_read.trim());
			}			
			out.close();
			conf.close();		
			producer.close();
		}catch (IOException e) {    	        
    	}catch (NullPointerException e) {    	        
    	}catch(InterruptedException e){
		}
	}
}

class Cmd{
	private Process process;
	private BufferedReader bufferedReader;
	private StringBuffer readBuffer;
			
	public String execCommand(String cmd) {
		try {
			process = Runtime.getRuntime().exec(cmd);
			bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			
			String line =null;
			readBuffer = new StringBuffer();
			
			while((line = bufferedReader.readLine()) != null) {
				readBuffer.append(line);
				System.out.println(line);
				readBuffer.append("\n");
			}
			return readBuffer.toString();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}













