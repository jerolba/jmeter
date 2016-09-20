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

package org.apache.jmeter.protocol.mongodb.mongo;

import java.util.List;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

/**
 */
public class MongoDB {

    private static final Logger log = LoggingManager.getLoggerForClass();

    // Mongo is Thread Safe
    private MongoClient mongo = null;
    
    public MongoDB(
            List<ServerAddress> serverAddresses,
            MongoClientOptions mongoOptions,
            List<MongoCredential> credentials) {
        mongo = new MongoClient(serverAddresses, credentials, mongoOptions);   
    }

    public MongoDatabase getDB(String database) {

        if(log.isDebugEnabled()) {
            log.debug("database: "+ database);
        }
        MongoDatabase db = mongo.getDatabase(database);
        return db;
    }

    public void clear() {
        if(log.isDebugEnabled()) {
            log.debug("clearing");
        }

        mongo.close();
        //there is no harm in trying to clear up
        mongo = null;
    }
}
