package tr.com.vodafone.ReadKafkaAndWriteCsvScheduled.Cron;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import lombok.SneakyThrows;

@Service
public class CronJob {

	
	
	
	@Autowired
	Environment environment;

	
	
	@SneakyThrows
	@Scheduled(cron = "0 1/2 * ? * *")
	@Async
	public void getDataFromKafkaAndWriteCsv() {
		String CSV_FILE_PATH = environment.getProperty("filePath");	
		System.out.println("Reading Kafka ....");
		
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("kafka.properties.kafka_server_address"));
		config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("kafka.properties.group_id_config"));
//			config.put(ConsumerConfig.CLIENT_ID_CONFIG, "deviceInfo");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.MAX_VALUE);
		config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.MAX_VALUE);
		config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.MAX_VALUE);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty("kafka.properties.AUTO_OFFSET_RESET_CONFIG"));

//	        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(config);
		  KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String,
		  String>(config);
		  kafkaConsumer.subscribe(Arrays.asList(environment.getProperty("kafka.properties.topic_name")));
		  BufferedWriter buffWriter = new BufferedWriter(new
		  FileWriter(CSV_FILE_PATH)); 
		  
		  try {
		  
		  ConsumerRecords<String, String> recordss = 
				  kafkaConsumer.poll(Duration.ofMillis(Long.parseLong(environment.getProperty("kafka.properties.timeoutOfMS")))); 
				  
		  
		  
		  if(!recordss.isEmpty()) { 
			  System.out.println("Reading Done ....");
			  System.out.println("count : " + recordss.count());
		  boolean checkHeader = true ;
		  String kafkaRecord ="";
		  for (ConsumerRecord<String, String> record : recordss) {
			  
			  kafkaRecord = new JSONObject(record.value().toString()).getString("VALUE");
			  if (checkHeader) {
				  if (!(kafkaRecord.startsWith("tac")  || kafkaRecord.startsWith("TAC") || kafkaRecord.startsWith("Tac"))) {
					  buffWriter.write("header1|header2|header3|header4|header5|header6|header7|header8|header9|header10" + System.lineSeparator());
					  System.out.println("Header Yok");
				}
				  checkHeader = false;
			  }
			  
			  buffWriter.write(kafkaRecord + System.lineSeparator());	
		  }


		  
		  buffWriter.flush(); 
		  buffWriter.close();
		  
		  System.out.println("Writing CSV ...."); 
		  
		  } 
		  else {
			  System.out.println("Topic is empty ....."); 
		  }
		  
		  System.out.println("Writing CSV DONE...."); } 
		  
		  catch (Exception e) {
//		  Logging 
			  System.out.println(e); 
		   } finally { 
			   kafkaConsumer.unsubscribe();
			   kafkaConsumer.close();
		  
		  
		  }
		 

		System.out.println("Finished...");
	}

}
