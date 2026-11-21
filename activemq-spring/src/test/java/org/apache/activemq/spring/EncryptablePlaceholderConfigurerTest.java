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
package org.apache.activemq.spring;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Properties;

import org.apache.activemq.util.ActiveMQEncryptor;
import org.apache.activemq.util.TextEncryptor;
import org.junit.Test;
import org.springframework.beans.factory.config.PropertyResourceConfigurer;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;

/**
 * Verifies that both encryptable placeholder configurers resolve plaintext,
 * legacy jasypt-format and current-format ENC() values from a properties
 * file into bean properties.
 */
public class EncryptablePlaceholderConfigurerTest {

    @Test
    public void testPropertyPlaceholderConfigurerVariant() {
        assertResolves("org/apache/activemq/spring/encryptable-ppc-context.xml");
    }

    @Test
    public void testPropertySourcesPlaceholderConfigurerVariant() {
        assertResolves("org/apache/activemq/spring/encryptable-pspc-context.xml");
    }

    @Test
    public void testCompositeAndNestedEncryptedProperties() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("activemq");
        var properties = new Properties();
        properties.setProperty("legacy", "ENC(4Pp68aOwkNEyvCcPBDCJAw==)");
        properties.setProperty("current", ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("password")));
        properties.setProperty("composite", "prefix-${legacy}-${current}-suffix");
        properties.setProperty("manager", "nested-success");
        for (var configurer : new PropertyResourceConfigurer[] {
                new EncryptablePropertyPlaceholderConfigurer(encryptor),
                new EncryptablePropertySourcesPlaceholderConfigurer(encryptor) }) {
            configurer.setProperties(properties);
            var target = resolve(configurer, "${composite}", "${${legacy}}", "${current}");
            assertEquals("prefix-manager-password-suffix", target.getPlain());
            assertEquals("nested-success", target.getLegacy());
            assertEquals("password", target.getCurrent());
        }
    }

    @Test
    public void testEncryptedEnvironmentPropertiesBeforeExpansion() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("activemq");
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("encrypted", Map.of(
                "secret", "ENC(4Pp68aOwkNEyvCcPBDCJAw==)",
                "manager", "nested-success")));
        var configurer = new EncryptablePropertySourcesPlaceholderConfigurer(encryptor);
        configurer.setEnvironment(environment);
        var target = resolve(configurer, "prefix-${secret}-suffix", "${${secret}}", "${secret}");
        assertEquals("prefix-manager-suffix", target.getPlain());
        assertEquals("nested-success", target.getLegacy());
        assertEquals("manager", target.getCurrent());
    }

    @Test
    public void testBareTokenEncryptorWithBothConfigurers() {
        TextEncryptor encryptor = new BareTokenEncryptor();
        var properties = new Properties();
        properties.setProperty("secret", ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("manager")));
        properties.setProperty("manager", "nested-success");
        for (var configurer : new PropertyResourceConfigurer[] {
                new EncryptablePropertyPlaceholderConfigurer(encryptor),
                new EncryptablePropertySourcesPlaceholderConfigurer(encryptor) }) {
            configurer.setProperties(properties);
            var target = resolve(configurer, "prefix-${secret}-suffix", "${${secret}}", "${secret}");
            assertEquals("prefix-manager-suffix", target.getPlain());
            assertEquals("nested-success", target.getLegacy());
            assertEquals("manager", target.getCurrent());
        }
    }

    @Test
    public void testExplicitPropertySourcesAndTypedDecryption() {
        var propertySources = new MutablePropertySources();
        propertySources.addLast(new MapPropertySource("encrypted", Map.of("number", "ENC(testv1:42)")));
        var configurer = new EncryptablePropertySourcesPlaceholderConfigurer(new BareTokenEncryptor());
        configurer.setPropertySources(propertySources);
        var properties = new Properties();
        properties.setProperty("number", "wrong-local-value");
        configurer.setProperties(properties);
        var target = resolve(configurer, "${number}", "${number}", "${number}");
        assertEquals("42", target.getPlain());
        assertEquals(Integer.valueOf(42), configurer.createPropertyResolver(propertySources).getProperty("number", Integer.class));
    }

    @Test
    public void testEncryptedPropertySourcePrecedence() {
        for (var localOverride : new boolean[] { false, true }) {
            var environment = new StandardEnvironment();
            environment.getPropertySources().addFirst(new MapPropertySource("encrypted", Map.of("secret", "ENC(testv1:environment)")));
            var configurer = new EncryptablePropertySourcesPlaceholderConfigurer(new BareTokenEncryptor());
            configurer.setEnvironment(environment);
            configurer.setLocalOverride(localOverride);
            var properties = new Properties();
            properties.setProperty("secret", "ENC(testv1:local)");
            configurer.setProperties(properties);
            var target = resolve(configurer, "${secret}", "${secret}", "${secret}");
            assertEquals(localOverride ? "local" : "environment", target.getPlain());
        }
    }

    private TargetBean resolve(PropertyResourceConfigurer configurer, String plain, String legacy, String current) {
        var factory = new DefaultListableBeanFactory();
        var definition = new RootBeanDefinition(TargetBean.class);
        definition.getPropertyValues().add("plain", plain).add("legacy", legacy).add("current", current);
        factory.registerBeanDefinition("target", definition);
        configurer.postProcessBeanFactory(factory);
        return factory.getBean("target", TargetBean.class);
    }

    private static class BareTokenEncryptor implements TextEncryptor {
        @Override
        public String encrypt(String value) {
            return "testv1:" + value;
        }

        @Override
        public String decrypt(String token) {
            if (!token.startsWith("testv1:")) {
                throw new IllegalArgumentException("Expected a bare token");
            }
            return token.substring("testv1:".length());
        }
    }

    private void assertResolves(String contextResource) {
        try (var context = new ClassPathXmlApplicationContext(contextResource)) {
            var target = context.getBean("target", TargetBean.class);
            assertEquals("plaintext", target.getPlain());
            assertEquals("manager", target.getLegacy());
            assertEquals("password", target.getCurrent());
        }
    }

    public static class TargetBean {

        private String plain;
        private String legacy;
        private String current;

        public String getPlain() {
            return plain;
        }

        public void setPlain(String plain) {
            this.plain = plain;
        }

        public String getLegacy() {
            return legacy;
        }

        public void setLegacy(String legacy) {
            this.legacy = legacy;
        }

        public String getCurrent() {
            return current;
        }

        public void setCurrent(String current) {
            this.current = current;
        }
    }
}
