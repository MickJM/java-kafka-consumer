package maersk.com.kafka.consumer;

import java.util.Map;

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

	@Value("${kafka.src.topic}")
	private String srcTopic;
	
	//@Autowired
	private DefaultKafkaConsumerFactory defaultKafka;
	
	private ConsumerFactory<?, ?> consumerFactory;
	//private DefaultKafkaConsumerFactory consumerFactory;
	
	public ProcessKafkaMessages(ConsumerFactory<?, ?> consumerFactory,
			DefaultKafkaConsumerFactory defaultKafka) {
		this.consumerFactory = consumerFactory;		
		this.defaultKafka = defaultKafka;		

	}
	//public ProcessKafkaMessages(DefaultKafkaConsumerFactory consumerFactory) {
	//	this.consumerFactory = consumerFactory;		
	//}

    @KafkaListener(topics = "${kafka.src.topic}")
    public void listen(@Payload String message) {
        System.out.println("received message=" + message);
    }

	
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
	
    
	//@Bean
    //public ConsumerFactory<Integer, String> consumerFactory() {
    //    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    //}

	/*
	@Bean
	public IntegrationFlow consumer() {
		
		
		IntegrationFlowBuilder bld = IntegrationFlows
				.from(Kafka
						.messageDrivenChannelAdapter(this.consumerFactory,
								KafkaMessageDrivenChannelAdapter.ListenerMode.record, srcTopic)
						.configureListenerContainer(c -> c
								.ackMode(AbstractMessageListenerContainer.AckMode.MANUAL)
				.get()));
								
								//.errorHandler(new SeekToCurrentErrorHandler())));
		//this.flowContext.registration(flow).register();		
		
		return null;
	}
	*/
	
}
