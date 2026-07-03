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


public class ActiveMQServerTextMessage extends AbstractActiveMQTextMessage implements TextMessage {

    @Override
    public Message copy() {
        ActiveMQServerTextMessage copy = new ActiveMQServerTextMessage();
        copy(copy);
        return copy;
    }

    @Override
    public void setText(String text) throws MessageNotWriteableException {
        checkReadOnlyBody();
        ByteSequence content = encodeContent(text);
        setContent(content);
        ActiveMQConnection connection = getConnection();
        if (connection != null && connection.isUseCompression()) {
            compressed = true;
        }
    }

    @Override
    public String getText() throws JMSException {
        ByteSequence content = getContent();
        return content != null ? decodeContent(content) : null;
    }

    @Override
    public String toString() {
        try {
            String text = decodeContent(getContent());
            if (text != null) {
                text = MarshallingSupport.truncate64(text);
                HashMap<String, Object> overrideFields = new HashMap<String, Object>();
                overrideFields.put("text", text);
                return super.toString(overrideFields);
            }
        } catch (JMSException e) {
        }
        return super.toString();
    }

    @SuppressWarnings("unchecked")
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        /*
         * If null the JMS spec says this method always returns true
         * regardless of the passed in class type.
         */
        if (getContent() == null) {
            return true;
        }
        return c.isAssignableFrom(java.lang.String.class);
    }

    @SuppressWarnings("unchecked")
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        return (T) getText();
    }
}
