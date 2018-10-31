package maersk.com.kafka.consumer;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlowBuilder;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.messaging.handler.annotation.Payload;

@Configuration
@EnableKafka
public class ProcessKafkaMessages {

	//@Value("${kafka.src.topic}")
	private String srcTopic;
	
	//private DefaultKafkaConsumerFactory<String,String> defaultKafka;	
	private ConsumerFactory<?, ?> consumerFactory;
	
	// Logger
    static Logger log = Logger.getLogger(ProcessKafkaMessages.class);

    /**
     * Constructor 
     * 
     * @param consumerFactory
     * @param defaultKafka
     */
	//public ProcessKafkaMessages(ConsumerFactory<?, ?> consumerFactory,
	//		DefaultKafkaConsumerFactory defaultKafka, String topicName) {
	public ProcessKafkaMessages(ConsumerFactory<?, ?> consumerFactory,
				String topicName) {
		
	//	this.consumerFactory = consumerFactory;		
	//	this.defaultKafka = defaultKafka;		
		this.srcTopic = topicName;
		log.info("TopicName = " + this.srcTopic);
		
	}

	/**
	 * Process any messages received ...
	 * @param consumerRecord
	 */
	@KafkaListener(topics = "${kafka.src.topic}")
    public void listen(ConsumerRecord<?,?> consumerRecord) {
        System.out.println("received message on " 
        					+ consumerRecord.topic() 
        					+ "- key:" 
        					+ consumerRecord.key()
        					+ " value: " + consumerRecord.value());

    }
	
	/*
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> 
							kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory 
        			= new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(this.defaultKafka);
        factory.setConcurrency(1);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }
    */
	
}
