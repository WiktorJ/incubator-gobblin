package org.apache.gobblin.kafka.client;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.KafkaHelper;
import org.apache.gobblin.util.ConfigUtils;

import java.util.Properties;

import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.KAFKA_SCHEMA_REGISTRY_SWITCH_NAME;
import static org.apache.gobblin.kafka.writer.KafkaWriterConfigurationKeys.KAFKA_SCHEMA_REGISTRY_SWITCH_NAME_DEFAULT;

/**
 * Created by wjurasz on 13.10.17.
 */
public class Kafka09ReaderHelper {

    private static final String KAFKA_09_CLIENT_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";
    private static final String KAFKA_09_CLIENT_ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit";
    private static final String KAFKA_09_CLIENT_SESSION_TIMEOUT_KEY = "session.timeout.ms";
    private static final String KAFKA_09_CLIENT_KEY_DESERIALIZER_CLASS_KEY = "key.deserializer";
    private static final String KAFKA_09_CLIENT_VALUE_DESERIALIZER_CLASS_KEY = "value.deserializer";

    private static final String KAFKA_09_DEFAULT_ENABLE_AUTO_COMMIT = Boolean.toString(false);
    private static final String KAFKA_09_DEFAULT_KEY_DESERIALIZER =
            "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String CONFIG_PREFIX = "source.kafka.";

    private static final String GOBBLIN_CONFIG_KEY_DESERIALIZER_CLASS_KEY = CONFIG_PREFIX
            + KAFKA_09_CLIENT_KEY_DESERIALIZER_CLASS_KEY;
    private static final String GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY = CONFIG_PREFIX
            + KAFKA_09_CLIENT_VALUE_DESERIALIZER_CLASS_KEY;

    static Properties getProducerProperties(Config config)
    {

        Preconditions.checkArgument(config.hasPath(GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY),
                "Missing required property " + GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY);

        Properties consumerProperties = KafkaHelper.stripPrefix(config, KAFKA_CONSUMER_CONFIG_PREFIX);
        KafkaHelper.setPropertyIfUnset(consumerProperties, KAFKA_09_CLIENT_BOOTSTRAP_SERVERS_KEY, Joiner.on(",").join(ConfigUtils.getStringList(config, ConfigurationKeys.KAFKA_BROKERS)));
        KafkaHelper.setPropertyIfUnset(consumerProperties, KAFKA_09_CLIENT_ENABLE_AUTO_COMMIT_KEY, KAFKA_09_DEFAULT_ENABLE_AUTO_COMMIT);
        KafkaHelper.setPropertyIfUnset(consumerProperties, KAFKA_09_CLIENT_SESSION_TIMEOUT_KEY, String.valueOf(AbstractBaseKafkaConsumerClient.CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE_DEFAULT));
        consumerProperties.put(KAFKA_09_CLIENT_KEY_DESERIALIZER_CLASS_KEY,
                ConfigUtils.getString(config, GOBBLIN_CONFIG_KEY_DESERIALIZER_CLASS_KEY, KAFKA_09_DEFAULT_KEY_DESERIALIZER));
        consumerProperties.put(KAFKA_09_CLIENT_VALUE_DESERIALIZER_CLASS_KEY,
                config.getString(GOBBLIN_CONFIG_VALUE_DESERIALIZER_CLASS_KEY));


        return consumerProperties;
    }
}
