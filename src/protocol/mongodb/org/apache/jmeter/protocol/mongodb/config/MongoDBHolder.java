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

import java.util.ArrayList;
import java.util.List;

import org.apache.jmeter.protocol.mongodb.mongo.MongoDB;
import org.apache.jmeter.threads.JMeterContextService;
import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.DB;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Public API to access MongoDB {@link DB} object created by
 * {@link MongoSourceElement}
 */
public final class MongoDBHolder {

    /**
     * Get access to MongoDB object
     * 
     * @param varName
     *            String MongoDB source
     * @param dbName
     *            Mongo DB database name
     * @return {@link DB}
     */
    public static MongoDatabase getDBFromSource(String varName, String dbName) {
        MongoDB mongodb = (MongoDB) JMeterContextService.getContext().getVariables().getObject(varName);
        if (mongodb == null) {
            throw new IllegalStateException(
                    "You didn't define variable:" + varName + " using MongoDB Source Config (property:MongoDB Source)");
        }
        return mongodb.getDB(dbName);
    }
    
    public static List<Document> consumeQuery(MongoCollection<Document> coll, BsonDocument query){
        FindIterable<Document> result = coll.find(query);
        MongoCursor<Document> iterator = result.batchSize(100).iterator();
        ArrayList<Document> docs = new ArrayList<>();
        if (iterator.hasNext()){
            docs.add(iterator.next());
        }
        return docs;
    }
}
