package maersk.com.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.util.StringUtils;

@Configuration
public class KafkaFactories {

	@Value("${kafka.src.bootstrap-servers:}")
	private String destBootstrapServers;
	@Value("${kafka.src.username:}")
	private String destUsername;
	@Value("${kafka.src.password:}")
	private String destPassword;
	@Value("${kafka.src.login-module:org.apache.kafka.common.security.plain.PlainLoginModule}")
	private String destLoginModule;
	@Value("${kafka.src.sasl-mechanism:PLAIN}")
	private String destSaslMechanism;
	@Value("${kafka.src.truststore-location:}")
	private String destTruststoreLocation;
	@Value("${kafka.src.truststore-password:}")
	private String destTruststorePassword;
	@Value("${kafka.src.linger:1}")
	private int destLinger;

    static Logger log = Logger.getLogger(KafkaFactories.class);

	@Bean
	public ConsumerFactory<Object, Object> consumerFactory() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destBootstrapServers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");
		//properties.put("transaction.timeout.ms", 5000);
		//properties.put("max.block.ms", 5000);

		//properties.put("acks", "1");
		properties.put("group.id", "kafka-test");
		properties.put("auto.offset.reset", "latest");
		properties.put("enable.auto.commit", "false");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("client.id", "kafka-consumer");

		log.info("Starting");
		addSaslProperties(properties, destSaslMechanism, destLoginModule, destUsername, destPassword);
		addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);

		return new DefaultKafkaConsumerFactory<>(properties);
	}

	private void addSaslProperties(Map<String, Object> properties, String mechanism, String loginModule, String username, String password) {
		if (!StringUtils.isEmpty(username)) {
			properties.put("security.protocol", "SASL_SSL");
			properties.put("sasl.mechanism", mechanism);
			properties.put("sasl.jaas.config",
					loginModule + " required username=" + username + " password=" + password + ";");
		}
		properties.put("ssl.keymanager.algorithm", "SunX509");

	}

	private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		if (!StringUtils.isEmpty(location)) {
			properties.put("ssl.truststore.location", location);
			properties.put("ssl.truststore.password", password);
		}
	}

	
}
