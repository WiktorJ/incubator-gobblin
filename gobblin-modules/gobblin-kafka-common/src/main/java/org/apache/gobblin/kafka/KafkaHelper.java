/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.kafka;

import java.util.Map;
import java.util.Properties;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

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
