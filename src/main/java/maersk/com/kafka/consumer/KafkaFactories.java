package maersk.com.kafka.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.util.StringUtils;

@Configuration
public class KafkaFactories {

	// Get all the parameters
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
	
	@Value("${spring.application.groupID:kafka-group}")
	private String groupId;	
	@Value("${spring.application.name:kafka-consumer}")
	private String clientId;
	@Value("${spring.application.concurrency:1}")
	private int concurrency;
	
	// logger
    static Logger log = Logger.getLogger(KafkaFactories.class);

    // Consumer factory
    //@Autowired
	//private ConsumerFactory<String, String> defaultKafka;	

	/**
	 * Get the topic name from the environment
	 * 
	 * @return
	 */
    @Bean
    public String GetTopic() {
		
    	String ret = null;
		try {
			ret = System.getenv("KAFKA_SRC_TOPIC");
    		log.info("TopicName have been overriden from Envirnonment Variables, using : " + ret);

		} catch (Exception e1) {
		}
    	return ret;
    }
    
    /**
     * Create a listener factory
     * 
     * @return
     */
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> 
							kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        
		ConcurrentKafkaListenerContainerFactory<String, String> factory 
        			= new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        factory.setConcurrency(this.concurrency);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    /**
     * Create a ConsumerFactory object, for connecting to Kafka
     * @return
     */
	@Bean
	//public ConsumerFactory<Object, Object> consumerFactory() {
	public ConsumerFactory<String, String> consumerFactory() {
		
		LoadRunTimeParameters();

		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, destBootstrapServers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		/**
		 * Not used for the consumer
		properties.put("transaction.timeout.ms", 5000);
		properties.put("max.block.ms", 5000);
		properties.put("acks", "1");
		 */
		
		//properties.put("auto.offset.reset", "latest");
		properties.put("auto.offset.reset", "earliest");

		properties.put("enable.auto.commit", "false");
		properties.put("auto.commit.interval.ms", "1000");

		SetGroupClientID();
		properties.put("group.id", this.groupId);
		properties.put("client.id", this.clientId);
		log.info("Starting Kafka-consumer");
		
		addSaslProperties(properties, destSaslMechanism, destLoginModule, destUsername, destPassword);
		addTruststoreProperties(properties, destTruststoreLocation, destTruststorePassword);

		//this.defaultKafka = new DefaultKafkaConsumerFactory<>(properties);
		return new DefaultKafkaConsumerFactory<>(properties);
		//return this.defaultKafka;
	}

	/**
	 * Set the GroupID and ClientID parameters
	 */
	private void SetGroupClientID() {
		try {
			if (this.groupId == null) {
				this.groupId = System.getenv("GROUPID");
			}
			if (this.clientId == null) {
				this.clientId = System.getenv("CLIENTID");
			}
			
			if (this.groupId == null) {
				this.groupId = "def-kafka-group";				
			}
			if (this.clientId == null) {
				this.clientId = "def-kafka-consumer";				
			}

    		log.info("GroupId/ClientId have been overriden from Envirnonment Variables");
    		
		} catch (Exception e1) {
			this.groupId = "def-kafka-group";
			this.clientId = "def-kafka-consumer";
		}
		log.info("GroupId : " + this.groupId);
		log.info("ClientId: " + this.clientId);
		
	}

	/**
	 * Add sasl SSL properties
	 * 
	 * @param properties
	 * @param mechanism
	 * @param loginModule
	 * @param username
	 * @param password
	 */
	private void addSaslProperties(Map<String, Object> properties, String mechanism, String loginModule, String username, String password) {
		if (!StringUtils.isEmpty(username)) {
			properties.put("security.protocol", "SASL_SSL");
			properties.put("sasl.mechanism", mechanism);
			properties.put("sasl.jaas.config",
					loginModule + " required username=" + username + " password=" + password + ";");
		}
		//properties.put("ssl.keymanager.algorithm", "SunX509");

	}

	/**
	 * Add a truststore
	 * 
	 * @param properties
	 * @param location
	 * @param password
	 */
	private void addTruststoreProperties(Map<String, Object> properties, String location, String password) {
		if (!StringUtils.isEmpty(location)) {
			properties.put("ssl.truststore.location", location);
			properties.put("ssl.truststore.password", password);
		}
	}

	/** 
	 * If using a parameter file to add parameters, load the values
	 * If using just USERID and PASSWORD, load them
	 */
	private void LoadRunTimeParameters() {

    	String fileName = null;
    	try {
    		fileName = System.getenv("PARAMS");
	    	File f = new File(fileName);
	    	if (!f.exists()) {	
	    		log.info("Properties file does not exist, using defaults");
	    		return;
	    	}
    	} catch (Exception e) {
    		log.info("PARAMS parameter is missing, using defaults");
    		try {
    			this.destUsername = System.getenv("USERID");
    			this.destPassword = System.getenv("PASSWORD");
        		log.info("USERID/PASSWORD have been overriden from Envirnonment Variables");

    		} catch (Exception e1) {
    		}
    		return;    		
    	}
    	
		log.info("Using property file: " + fileName);

		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream(fileName);
			prop.load(input);
			
			this.destBootstrapServers = prop.getProperty("kafka.dest.bootstrap-servers");
			this.destUsername = prop.getProperty("kafka.dest.username");
			this.destPassword = prop.getProperty("kafka.dest.password");
			this.destLoginModule = prop.getProperty("kafka.dest.login-module");
			this.destSaslMechanism = prop.getProperty("kafka.dest.sasl-mechanism");
			this.destTruststoreLocation = prop.getProperty("kafka.dest.truststore-location");
			this.destTruststorePassword = prop.getProperty("kafka.dest.truststore-password");
			this.destLinger = Integer.parseInt(prop.getProperty("kafka.dest.linger"));
			this.clientId = prop.getProperty("spring.application.name");
			this.groupId = prop.getProperty("spring.application.groupId");
			try {
				this.concurrency = Integer.parseInt(prop.getProperty("spring.application.concurrency"));
			} catch (Exception e) {
				log.error("Concurrecny defaulting to 1 ");
				this.concurrency = 1;
			}
		} catch(IOException e) {
			log.error("IOException error : " + e.getMessage());
			System.exit(1);
			
		} finally {
			
	        if (input != null) {
	            try {
	                input.close();
	                
	            } catch (IOException e) {    	
	    			// do nothing
	            
	            }
	        }
	    }
	}
}
