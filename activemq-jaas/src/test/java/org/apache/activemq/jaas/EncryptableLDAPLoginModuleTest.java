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
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import javax.security.auth.Subject;

import org.apache.activemq.util.ActiveMQEncryptor;
import org.junit.Test;

public class EncryptableLDAPLoginModuleTest {

    @Test
    public void testEncryptionSettingsRemainLiteralAndBindPasswordIsDecrypted() throws Exception {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("ENC(master)");
        encryptor.setIterations(ActiveMQEncryptor.MIN_ITERATIONS);
        var encryptedBindPassword = ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("bind-secret"));
        var options = new HashMap<String, String>();
        options.put("encryptionPassword", "ENC(master)");
        options.put("passwordEnvName", "ENC(literal-env-name)");
        options.put("encryptionAlgorithm", "ENC(literal-algorithm)");
        options.put("connectionPassword", encryptedBindPassword);
        options.put("connectionUsername", "cn=admin");
        options.put("initialContextFactory", InspectingContextFactory.class.getName());
        options.put("connectionURL", "ldap://localhost:389");
        options.put("connectionProtocol", "");
        options.put("authentication", "simple");
        var module = new EncryptableLDAPLoginModule();
        module.initialize(new Subject(), null, new HashMap<>(), options);

        try {
            module.openContext();
            fail("Expected the test context factory to stop before LDAP I/O");
        } catch (NamingException expected) {
            assertEquals("Bind credentials verified", expected.getMessage());
        }
        assertEquals("ENC(master)", options.get("encryptionPassword"));
        assertEquals("ENC(literal-env-name)", options.get("passwordEnvName"));
        assertEquals("ENC(literal-algorithm)", options.get("encryptionAlgorithm"));
        assertEquals(encryptedBindPassword, options.get("connectionPassword"));
    }

    public static class InspectingContextFactory implements InitialContextFactory {
        @Override
        public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
            assertEquals("cn=admin", environment.get(Context.SECURITY_PRINCIPAL));
            assertEquals("bind-secret", environment.get(Context.SECURITY_CREDENTIALS));
            throw new NamingException("Bind credentials verified");
        }
    }
}
