package tr.com.vodafone.ReadKafkaAndWriteCsvScheduled;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ReadKafkaAndWriteCsvScheduledApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReadKafkaAndWriteCsvScheduledApplication.class, args);
	}

}
