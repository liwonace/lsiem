import java.io.*;
import java.lang.*;
import java.io.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import kafka.producer.KeyedMessage;
import java.util.Properties;

/* .bashrc

 export KAFKA_LIB='/opt/kafka_2.9.1-0.8.2.1/libs'

 export CLASSPATH=.:$KAFKA_LIB/jopt-simple-3.2.jar:$KAFKA_LIB/kafka-clients-0.8.2.1.jar

                   :$KAFKA_LIB/kafka_2.9.1-0.8.2.1.jar:$KAFKA_LIB/log4j-1.2.16.jar

                   :$KAFKA_LIB/metrics-core-2.2.0.jar:$KAFKA_LIB/scala-library-2.9.1.jar

                   :$KAFKA_LIB/slf4j-api-1.7.6.jar:$KAFKA_LIB/slf4j-log4j12-1.6.1.jar

                   :$KAFKA_LIB/snappy-java-1.1.1.6.jar:$KAFKA_LIB/zkclient-0.3.jar

                   :$KAFKA_LIB/zookeeper-3.4.6.jar
*/
public class Producer {

	public static void main(String[] args) throws IOException {

		String[] log_list = new String[10];
		int log_count = 0;
		String s = null;

		BufferedReader conf = new BufferedReader(new FileReader("/var/log/messages"));
		while ((s = conf.readLine()) != null) {
			if (s.length() > 0 && log_count < 10) {
				log_list[log_count] = s;
				System.out.println(s);
				log_count++;
			}
		}
		conf.close();

		Properties configs = new Properties();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("acks", "all");
		configs.put("block.on.buffer.full", "true");
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
		//List<KeyedMessage<String, String>> messages = new ArrayList<KeyedMessage<String, String>>();
		for (int i = 0; i < 10; i++) { 
			producer.send(new ProducerRecord<>("test", log_list[i]),
				(metadata, exception) -> {
					if (metadata != null) {
						System.out.println(
							"partition(" + metadata.partition() + "), offset(" + metadata.offset() + ")");
					} else {
						exception.printStackTrace();
					}
				});

			producer.flush();
		}
		producer.close();
	}
}