package com.daluga.jms.topic.subscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.jms.*;

@SpringBootApplication
public class JmsTopicSubscriberApplication implements CommandLineRunner {

    private static final String CLIENT_ID = "123";

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsTopicSubscriberApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(JmsTopicSubscriberApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		LOGGER.debug("JmsTopicSubscriberApplication Has Started!");

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);

        Connection connection = connectionFactory.createConnection();
        connection.setClientID(CLIENT_ID);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Topic topic = session.createTopic("DD00865.TOPIC");

        //MessageConsumer messageConsumer = session.createConsumer(topic);
        MessageConsumer messageConsumer = session.createDurableSubscriber(topic, "MY_SUBSCRIPTION_NAME");

        connection.start();

        Message message = messageConsumer.receive(30000);

        if (message != null) {
            // cast the message to the correct type
            TextMessage textMessage = (TextMessage) message;

            // retrieve the message content
            String text = textMessage.getText();
            LOGGER.debug(CLIENT_ID + ": received message with text='{}'", text);

            // create greeting
            String greeting = "Hello " + text + "!";
            LOGGER.info("greeting={}", greeting);
        } else {
            LOGGER.debug(CLIENT_ID + ": no message received");
        }

        connection.close();

        LOGGER.debug("JmsTopicSubscriberApplication Has Ended!");

	}
}
