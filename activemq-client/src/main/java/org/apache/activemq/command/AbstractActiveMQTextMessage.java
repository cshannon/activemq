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
package org.apache.activemq.command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import jakarta.jms.JMSException;
import jakarta.jms.MessageNotWriteableException;
import jakarta.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;


public abstract class AbstractActiveMQTextMessage extends ActiveMQMessage implements TextMessage {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_TEXT_MESSAGE;

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public String getJMSXMimeType() {
        return "jms/text-message";
    }

    protected String decodeContent(ByteSequence bodyAsBytes) throws JMSException {
        String text = null;
        if (bodyAsBytes != null) {
            InputStream is = null;
            try {
                is = new ByteArrayInputStream(bodyAsBytes);
                if (isCompressed()) {
                    // wrap the stream so we don't inflate past maxInflatedDataSize
                    is = MarshallingSupport.createInflaterInputStream(getMaxInflatedDataSize(), is);
                }
                DataInputStream dataIn = new DataInputStream(is);
                text = MarshallingSupport.readUTF8(dataIn);
                dataIn.close();
            } catch (IOException ioe) {
                throw JMSExceptionSupport.create(ioe);
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
        return text;
    }

    protected ByteSequence encodeContent(String text) {
        final ByteSequence content;

        try {
            if (text == null) {
                content = null;
            } else {

                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                OutputStream os = bytesOut;
                ActiveMQConnection connection = getConnection();
                if (connection != null && connection.isUseCompression()) {
                    //    compressed = true;
                    os = new DeflaterOutputStream(os);
                }
                DataOutputStream dataOut = new DataOutputStream(os);
                MarshallingSupport.writeUTF8(dataOut, text);
                dataOut.close();
                content = bytesOut.toByteSequence();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return content;
    }
}
