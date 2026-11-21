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
package org.apache.activemq.jaas;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.activemq.util.TextEncryptor;
import org.junit.Test;

public class EncryptionSupportTest {

    @Test
    public void testGenericEncryptorReceivesBareToken() {
        var properties = new Properties();
        properties.setProperty("encrypted", "  ENC(testv1:decoded)  ");
        properties.setProperty("plain", "plaintext");
        TextEncryptor encryptor = new TextEncryptor() {
            @Override
            public String encrypt(String plaintext) {
                return "testv1:" + plaintext;
            }

            @Override
            public String decrypt(String token) {
                if (!token.startsWith("testv1:")) {
                    throw new IllegalArgumentException("Expected a bare token");
                }
                return token.substring("testv1:".length());
            }
        };

        EncryptionSupport.decrypt(properties, encryptor);

        assertEquals("decoded", properties.getProperty("encrypted"));
        assertEquals("plaintext", properties.getProperty("plain"));
    }
}
