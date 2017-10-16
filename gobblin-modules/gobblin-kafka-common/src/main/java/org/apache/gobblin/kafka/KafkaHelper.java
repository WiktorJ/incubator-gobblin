package org.apache.gobblin.kafka;

import java.util.Map;
import java.util.Properties;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Created by wjurasz on 13.10.17.
 */
public class KafkaHelper {

    public static Properties stripPrefix(Properties props, String prefix) {
        Properties strippedProps = new Properties();
        int prefixLength = prefix.length();
        for (String key: props.stringPropertyNames())
        {
            if (key.startsWith(prefix))
            {
                strippedProps.setProperty(key.substring(prefixLength), props.getProperty(key));
            }
        }
        return strippedProps;
    }

    public static Properties stripPrefix(Config config, String prefix) {
        Properties strippedProps = new Properties();
        int prefixLength = prefix.length();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            {
                strippedProps.setProperty(entry.getKey().substring(prefixLength), config.getString(entry.getKey()));
            }
        }
        return strippedProps;
    }

    public static void setPropertyIfUnset(Properties props, String key, String value)
    {
        if (!props.containsKey(key)) {
            props.setProperty(key, value);
        }
    }
}
