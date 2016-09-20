/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jmeter.protocol.mongodb.config;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TypeEditor;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
  */
public class MongoSourceElementBeanInfo extends BeanInfoSupport {

    private static final Logger log = LoggingManager.getLoggerForClass();

    public MongoSourceElementBeanInfo() {
        super(MongoSourceElement.class);

        //https://api.mongodb.com/java/3.2/com/mongodb/Mongo.html
        createPropertyGroup("mongodb", new String[] {
                "connection",
                "source"});

        //https://api.mongodb.com/java/3.2/com/mongodb/MongoCredential.html
        createPropertyGroup("credential", new String[]{
                "database",
                "username",
                "password"
        });
        
        
        //https://api.mongodb.com/java/3.2/com/mongodb/MongoClientOptions.html
        createPropertyGroup("connectionPool", new String[]{
                "minConnectionsPerHost",
                "maxConnectionsPerHost",
                "maxWaitTime",
                "maxConnectionIdleTime",
                "maxConnectionLifeTime",
                "threadsAllowedToBlockForConnectionMultiplier"
        });
        
        createPropertyGroup("socketSettings", new String[]{
                "connectTimeout",
                "socketTimeout",
                "socketKeepAlive"
        });
        
        createPropertyGroup("heartbeat", new String[]{
                "heartbeatConnectTimeout",
                "heartbeatSocketTimeout",
                "heartbeatFrequency",
                "minHeartbeatFrequency"
        });
        
        createPropertyGroup("ssl", new String[]{
                "sslEnabled",
                "sslInvalidHostNameAllowed"
        });

        //https://api.mongodb.com/java/3.2/com/mongodb/WriteConcern.html
        createPropertyGroup("writeConcern", new String[] {
                "writers",
                "writeTimeout",
                "fsync",
                "journal"
        });
        
        
        describeRequired("connection", "");
        describeRequired("source", "");
        
        describe("database");
        describe("username");
        PropertyDescriptor p = property("password", TypeEditor.PasswordEditor);
        p.setValue(NOT_UNDEFINED, false);
        p.setValue(DEFAULT, "");

        describeRequired("minConnectionsPerHost", 0);
        describeRequired("maxConnectionsPerHost", 100);
        describeRequired("maxWaitTime", 1000 * 60 * 2);
        describeRequired("maxConnectionIdleTime", 0);
        describeRequired("maxConnectionLifeTime", 0);
        describeRequired("threadsAllowedToBlockForConnectionMultiplier", 5);
        
        describeRequired("connectTimeout", 1000 * 10);
        describeRequired("socketTimeout", 0);
        describeRequired("socketKeepAlive", false);
        
        describeRequired("heartbeatConnectTimeout", 20000);
        describeRequired("heartbeatSocketTimeout", 20000);
        describeRequired("heartbeatFrequency", 10000);
        describeRequired("minHeartbeatFrequency", 500);
        
        describeRequired("sslEnabled", false);
        describeRequired("sslInvalidHostNameAllowed", false);
        
        describeRequired("writers", 1);
        describe("writeTimeout");
        describe("fsync",false);
        describe("journal",false);
        
        if(log.isDebugEnabled()) {
            for (PropertyDescriptor pd : getPropertyDescriptors()) {
                log.debug(pd.getName());
                log.debug(pd.getDisplayName());
            }
        }
    }
    
    private PropertyDescriptor describeRequired(String name, Object defaultValue) {
        PropertyDescriptor p = property(name);
        p.setValue(NOT_UNDEFINED, true);
        p.setValue(DEFAULT, defaultValue);
        return p;
    }

    private PropertyDescriptor describe(String name, Object defaultValue) {
        PropertyDescriptor p = property(name);
        p.setValue(NOT_UNDEFINED, false);
        p.setValue(DEFAULT, defaultValue);
        return p;
    }

    private PropertyDescriptor describe(String name) {
        PropertyDescriptor p = property(name);
        p.setValue(NOT_UNDEFINED, false);
        return p;
    }

}
