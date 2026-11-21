/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.activemq.plugin.PropertiesPlaceHolderUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PropertiesPlaceHolderUtilTest {

    private static final String BEANS_NAMESPACE = "http://www.springframework.org/schema/beans";

    @Parameterized.Parameters(name = "{0}, localOverride={1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
            {"org.springframework.beans.factory.config.PropertyPlaceholderConfigurer", false},
            {"org.springframework.beans.factory.config.PropertyPlaceholderConfigurer", true},
            {"org.springframework.context.support.PropertySourcesPlaceholderConfigurer", false},
            {"org.springframework.context.support.PropertySourcesPlaceholderConfigurer", true},
            {"org.apache.activemq.spring.EncryptablePropertyPlaceholderConfigurer", false},
            {"org.apache.activemq.spring.EncryptablePropertyPlaceholderConfigurer", true},
            {"org.apache.activemq.spring.EncryptablePropertySourcesPlaceholderConfigurer", false},
            {"org.apache.activemq.spring.EncryptablePropertySourcesPlaceholderConfigurer", true}
        });
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String configurerClass;
    private final boolean localOverride;

    public PropertiesPlaceHolderUtilTest(String configurerClass, boolean localOverride) {
        this.configurerClass = configurerClass;
        this.localOverride = localOverride;
    }

    @Test
    public void testMergeFileProperties() throws Exception {
        File propertyFile = temporaryFolder.newFile("broker.properties");
        Properties fileProperties = new Properties();
        fileProperties.setProperty("broker.name", "file-backed-broker");
        fileProperties.setProperty("precedence", "file");
        try (OutputStream output = new FileOutputStream(propertyFile)) {
            fileProperties.store(output, null);
        }

        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
        Element beans = document.createElementNS(BEANS_NAMESPACE, "beans");
        document.appendChild(beans);
        Element bean = document.createElementNS(BEANS_NAMESPACE, "bean");
        bean.setAttribute("class", configurerClass);
        beans.appendChild(bean);
        addProperty(document, bean, "locations", propertyFile.toURI().toString());
        addProperty(document, bean, "localOverride", Boolean.toString(localOverride));

        Properties properties = new Properties();
        properties.setProperty("precedence", "initial");
        properties.setProperty("initial.only", "retained");
        PropertiesPlaceHolderUtil util = new PropertiesPlaceHolderUtil(properties);
        util.mergeProperties(document, properties, null);

        assertEquals("file-backed-broker", properties.getProperty("broker.name"));
        assertEquals("file-backed-broker", util.filter("${broker.name}"));
        assertEquals("retained", properties.getProperty("initial.only"));
        // Runtime reload has always merged file values over initial values,
        // independently of the Spring configurer's localOverride setting.
        assertEquals("file", properties.getProperty("precedence"));
    }

    private static void addProperty(Document document, Element bean, String name, String value) {
        Element property = document.createElementNS(BEANS_NAMESPACE, "property");
        property.setAttribute("name", name);
        Element propertyValue = document.createElementNS(BEANS_NAMESPACE, "value");
        propertyValue.appendChild(document.createTextNode(value));
        property.appendChild(propertyValue);
        bean.appendChild(property);
    }
}
