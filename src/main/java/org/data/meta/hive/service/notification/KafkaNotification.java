package org.data.meta.hive.service.notification;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.data.meta.hive.exceptions.NotificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka specific access point to the notification framework
 *
 * @author chenchaolin
 * @date 2023-07-02
 */
public class KafkaNotification extends AbstractNotification {

    public static final Logger LOG = LoggerFactory.getLogger(KafkaNotification.class);

    public static final String HOOK_TOPIC = "TOPIC_METADATA_LINEAGE";
    private final Properties properties;
    private KafkaProducer<String, String> producer;

    /**
     * Construct a KafkaNotification.
     */
    public KafkaNotification() throws LoginException {
        //Kerberos认证
        Configuration.setConfiguration(new JaasConfiguration());
        LoginContext loginContext = new LoginContext("KafkaClient");
        loginContext.login();

        properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.231.3.123:6667,10.231.3.124:6667,10.231.3.125:6667");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 设置Kerberos相关的配置
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_MECHANISM, "GSSAPI");
        properties.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka");
    }


    @Override
    protected void sendInternal(List<String> messages) {
        if (producer == null) {
            createProducer();
        }
        sendInternalToProducer(producer, messages);

    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }

    }

    void sendInternalToProducer(Producer<String, String> p, List<String> messages) {
        //往kafka发消息callback的message信息
        List<MessageContext> messageContexts = new ArrayList<>();

        for (String message : messages) {
            if (StringUtils.isNotBlank(message)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(HOOK_TOPIC, message);

//                if (LOG.isDebugEnabled()) {
//                    LOG.debug("Sending message for topic {}: {}", HOOK_TOPIC, message);
//                }
                LOG.info("Sending message for topic {}: {}", HOOK_TOPIC, message);

                //往kafka推消息，这里没用带callback的方法,发完再统一处理
                Future<RecordMetadata> future = p.send(record);

                //收集发送到kafka的message的回调信息
                messageContexts.add(new MessageContext(future, message));
            }
        }

        List<String> failedMessages = new ArrayList<>();
        Exception lastFailureException = null;

        //处理发送到kafka的message的回调信息
        for (MessageContext context : messageContexts) {
            try {
                RecordMetadata response = context.getFuture().get();

//                if (LOG.isDebugEnabled()) {
//                    LOG.debug("Sent message for topic - {}, partition - {}, offset - {}", response.topic(), response.partition(), response.offset());
//                }
                LOG.info("Sent message for topic - {}, partition - {}, offset - {}", response.topic(), response.partition(), response.offset());
            } catch (Exception e) {
                lastFailureException = e;
                //失败信息
                failedMessages.add(context.getMessage());
            }
        }

        if (lastFailureException != null) {
            throw new NotificationException(lastFailureException, failedMessages);
        }
        //关闭客户端
        close();
    }

    /**
     * kafka 生产者
     */
    private synchronized void createProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(properties);
        }
    }

    /**
     * 发送到kafka的message的回调信息
     */
    private static class MessageContext {
        private final Future<RecordMetadata> future;
        private final String message;

        public MessageContext(Future<RecordMetadata> future, String message) {
            this.future = future;
            this.message = message;
        }

        public Future<RecordMetadata> getFuture() {
            return future;
        }

        public String getMessage() {
            return message;
        }
    }


}
