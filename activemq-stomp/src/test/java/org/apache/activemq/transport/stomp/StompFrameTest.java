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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.MarshallingSupport;
import org.junit.Test;

public class StompFrameTest {
    StompFrame underTest = new StompFrame();

    @Test
    public void testNoPasscodeInToString() throws Exception {
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put("userName", "bob");
        headers.put("passcode", "please");
        underTest.setHeaders(headers);

        assertEquals("no password present", -1, underTest.toString().indexOf("please"));
        assertTrue("*** present", underTest.toString().indexOf("***") > 0);
    }

    @Test
    public void testEncoding() throws Exception {
        String text = "!®౩\uD83D\uDE42";
        StompFrame testFrame = new StompFrame();

        ByteSequence encoded;
        try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytesOut)) {
            MarshallingSupport.writeUTF8(dataOut, text);
            encoded = bytesOut.toByteSequence();
            testFrame.setContent(encoded.getData());
        }

        // verify stomp can write/read back original string
        assertEquals(text, testFrame.getBody());

        // Verify compatible with ActiveMQTextMessage
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setContent(encoded);
        assertEquals(text, message.getText());

        // Re-encode with ActivemqTextMessage and verify byte array
        message = new ActiveMQTextMessage();
        message.setText(text);
        message.beforeMarshall(new OpenWireFormat());
        assertArrayEquals(encoded.getData(), message.getContent().getData());

        // Re-encode with ActivemqTextMessage and decode with Stomp
        testFrame = new StompFrame();
        testFrame.setContent(message.getContent().getData());
        assertEquals(text, testFrame.getBody());
    }
}
