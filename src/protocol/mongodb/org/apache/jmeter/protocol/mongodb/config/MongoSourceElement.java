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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.protocol.mongodb.mongo.MongoDB;
import org.apache.jmeter.protocol.mongodb.mongo.MongoUtils;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.WriteConcern;

/**
 */
public class MongoSourceElement extends ConfigTestElement implements TestStateListener, TestBean {

    /**
     * 
     */
    private static final long serialVersionUID = 2100L;

    private static final Logger log = LoggingManager.getLoggerForClass();

    private String connection;
    private String source;

    private String database;
    private String username;
    private String password;

    private int minConnectionsPerHost;
    private int maxConnectionsPerHost;
    private int maxWaitTime;
    private int maxConnectionIdleTime;
    private int maxConnectionLifeTime;
    private int threadsAllowedToBlockForConnectionMultiplier;
    private int connectTimeout;
    private int socketTimeout;
    private boolean socketKeepAlive;
    private int heartbeatConnectTimeout;
    private int heartbeatSocketTimeout;
    private int heartbeatFrequency;
    private int minHeartbeatFrequency;
    private boolean sslEnabled;
    private boolean sslInvalidHostNameAllowed;
    private int writers;
    private int writeTimeout;
    private Boolean fsync = false;
    private Boolean journal = false;

    public String getTitle() {
        return this.getName();
    }

    public static MongoDB getMongoDB(String source) {

        Object mongoSource = JMeterContextService.getContext().getVariables().getObject(source);

        if (mongoSource == null) {
            throw new IllegalStateException("mongoSource is null");
        } else {
            if (mongoSource instanceof MongoDB) {
                return (MongoDB) mongoSource;
            } else {
                throw new IllegalStateException(
                        "Variable:" + source + " is not a MongoDB instance, class:" + mongoSource.getClass());
            }
        }
    }

    @Override
    public void addConfigElement(ConfigElement configElement) {
    }

    @Override
    public boolean expectsModification() {
        return false;
    }

    @Override
    public void testStarted() {
        if (log.isDebugEnabled()) {
            log.debug(getTitle() + " testStarted");
        }

        if (getThreadContext().getVariables().getObject(getSource()) != null) {
            if (log.isWarnEnabled()) {
                log.warn(getSource() + " has already been defined.");
            }
            return;
        }

        MongoClientOptions.Builder builder = MongoClientOptions.builder().minConnectionsPerHost(minConnectionsPerHost)
                .connectionsPerHost(maxConnectionsPerHost).maxWaitTime(maxWaitTime)
                .maxConnectionIdleTime(maxConnectionIdleTime).maxConnectionLifeTime(maxConnectionLifeTime)
                .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
                .connectTimeout(connectTimeout).socketTimeout(socketTimeout).socketKeepAlive(socketKeepAlive)
                .heartbeatConnectTimeout(heartbeatConnectTimeout).heartbeatSocketTimeout(heartbeatSocketTimeout)
                .heartbeatFrequency(heartbeatFrequency).minHeartbeatFrequency(minHeartbeatFrequency)
                .sslEnabled(sslEnabled).sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);

        WriteConcern wc = new WriteConcern(writers).withWTimeout(writeTimeout, TimeUnit.MILLISECONDS)
                .withFsync(fsync == null ? false : fsync).withJournal(journal == null ? false : journal);

        builder.writeConcern(wc);

        List<MongoCredential> credentials = new ArrayList<>();
        if (StringUtils.isNoneBlank(database, username, password)) {
            MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
            credentials.add(credential);
        }

        MongoClientOptions mongoOptions = builder.build();

        if (log.isDebugEnabled()) {
            log.debug("options : " + mongoOptions.toString());
        }

        if (log.isDebugEnabled()) {
            log.debug(getSource() + "  is being defined.");
        }
        try {
            getThreadContext().getVariables().putObject(getSource(),
                    new MongoDB(MongoUtils.toServerAddresses(getConnection()), mongoOptions, credentials));
        } catch (UnknownHostException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void testStarted(String s) {
        testStarted();
    }

    @Override
    public void testEnded() {
        if (log.isDebugEnabled()) {
            log.debug(getTitle() + " testEnded");
        }
        ((MongoDB) getThreadContext().getVariables().getObject(getSource())).clear();
    }

    @Override
    public void testEnded(String s) {
        testEnded();
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    public void setMinConnectionsPerHost(int minConnectionsPerHost) {
        this.minConnectionsPerHost = minConnectionsPerHost;
    }

    public int getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    public void setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    public void setMaxConnectionLifeTime(int maxConnectionLifeTime) {
        this.maxConnectionLifeTime = maxConnectionLifeTime;
    }

    public int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    public void setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
        this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {
        this.socketKeepAlive = socketKeepAlive;
    }

    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {
        this.heartbeatConnectTimeout = heartbeatConnectTimeout;
    }

    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {
        this.heartbeatSocketTimeout = heartbeatSocketTimeout;
    }

    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public void setHeartbeatFrequency(int heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public int getMinHeartbeatFrequency() {
        return minHeartbeatFrequency;
    }

    public void setMinHeartbeatFrequency(int minHeartbeatFrequency) {
        this.minHeartbeatFrequency = minHeartbeatFrequency;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }

    public void setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
        this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    }

    public int getWriters() {
        return writers;
    }

    public void setWriters(int writers) {
        this.writers = writers;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public Boolean getFsync() {
        return fsync;
    }

    public void setFsync(Boolean fsync) {
        this.fsync = fsync;
    }

    public Boolean getJournal() {
        return journal;
    }

    public void setJournal(Boolean journal) {
        this.journal = journal;
    }

}
