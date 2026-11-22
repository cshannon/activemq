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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import javax.management.ObjectName;

import jakarta.jms.JMSException;
import jakarta.jms.JMSSecurityException;
import jakarta.jms.JMSSecurityRuntimeException;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Jakarta Messaging expects createConnection and createContext to authenticate
 * the caller immediately. ActiveMQ defers the ConnectionInfo exchange until
 * first use, so the eager handshake is enabled by strictCompliance.
 */
public class StrictComplianceAuthenticationTest {

    private BrokerService broker;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setBrokerName("strict-authentication");
        broker.setUseJmx(true);
        broker.getManagementContext().setCreateConnector(false);
        broker.setAdvisorySupport(false);
        var users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser("system", "manager", "users,admins"));
        broker.setPlugins(new BrokerPlugin[] {new SimpleAuthenticationPlugin(users)});
        broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateConnectionRejectsBadCredentialsImmediately() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        try {
            factory.createConnection("invalid", "credentials");
            fail("Expected JMSSecurityException from createConnection");
        } catch (JMSSecurityException expected) {
        }

        try (var connection = factory.createConnection("system", "manager")) {
            connection.start();
            assertNotNull(connection.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateContextRejectsBadCredentialsImmediately() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        try {
            factory.createContext("invalid", "credentials");
            fail("Expected JMSSecurityRuntimeException from createContext");
        } catch (JMSSecurityRuntimeException expected) {
        }

        try (var context = factory.createContext("system", "manager")) {
            context.start();
            assertNotNull(context.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testStrictCreateConnectionStillAllowsSetClientID() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);

        // the spec-mandated sequence: set the client identifier immediately after
        // creation, before any other action; eager authentication must not
        // consume the connection's identity
        try (var connection = factory.createConnection("system", "manager")) {
            connection.setClientID("strict-client");
            connection.start();
            assertEquals("strict-client", connection.getClientID());
        }
    }

    @Test(timeout = 60000)
    public void testStrictConnectionRegistersActualClientIDInJmx() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);
        factory.setStrictCompliance(true);
        var connectionNames = new ObjectName(broker.getBrokerObjectName()
                + ",connector=clientConnectors,connectorName=*,connectionViewType=clientId,connectionName=*");
        var connectorName = BrokerMBeanSupport.createConnectorName(broker.getBrokerObjectName(),
                "clientConnectors", broker.getTransportConnectors().get(0).getName());
        var expectedName = BrokerMBeanSupport.createConnectionViewByType(connectorName, "clientId", "strict-jmx-client");

        try (var connection = factory.createConnection("system", "manager")) {
            connection.setClientID("strict-jmx-client");
            // setClientID waits for the real ConnectionInfo response, so the
            // preceding authentication probe removal has also been processed.
            connection.start();

            var registeredNames = broker.getManagementContext().queryNames(connectionNames, null);
            assertEquals("Only the actual client ID should be registered, not the authentication probe", 1,
                    registeredNames.size());
            assertTrue("The actual client ID must be visible through JMX", registeredNames.contains(expectedName));
            assertEquals("strict-jmx-client", broker.getManagementContext().getMBeanServer()
                    .getAttribute(expectedName, "ClientId"));
        }

        assertTrue("Closing the connection must remove its client ID registration",
                broker.getManagementContext().queryNames(connectionNames, null).isEmpty());
    }

    @Test(timeout = 60000)
    public void testLegacyCreateConnectionDefersAuthenticationToStart() throws Exception {
        var factory = new ActiveMQConnectionFactory(connectionUri);

        // Default strictCompliance = false: creation succeeds, the failure surfaces
        // when the connection first talks to the broker
        var connection = factory.createConnection("invalid", "credentials");
        try {
            connection.start();
            fail("Expected JMSSecurityException on start");
        } catch (JMSSecurityException expected) {
        } catch (JMSException disposed) {
            // the failed connection may be torn down asynchronously before the
            // security exception reaches start(); either form indicates rejection
        } finally {
            try { connection.close(); } catch (Exception ignored) {}
        }
    }
}
