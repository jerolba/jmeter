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

package org.apache.jmeter.protocol.mongodb.sampler;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoDatabase;

/**
 */
public class MongoScriptRunner {

    public MongoScriptRunner() {
        super();
    }

    /**
     * Evaluate a command on the database
     *
     * @param db
     *            database connection to use
     * @param script
     *            document with command to evaluate on the database
     * @return result of evaluation on the database
     * @throws Exception
     *             when evaluation on the database fails
     */

    public Document command(MongoDatabase db, String document) throws Exception {
        BsonDocument bsonDocument = BsonDocument.parse(document);
        Document result = db.runCommand(bsonDocument);
        return result;
    }
}
