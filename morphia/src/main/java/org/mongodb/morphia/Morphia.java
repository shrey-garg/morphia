/*
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */


package org.mongodb.morphia;


import com.mongodb.MongoClient;
import org.mongodb.morphia.mapping.Mapper;

import java.util.Set;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


public class Morphia {
    private final Mapper mapper;
    private MongoClient mongoClient;

    /**
     * Creates a Morphia instance with a default Mapper and an empty class set.
     *
     * @param mongoClient the representations of the connection to a MongoDB instance
     */
    public Morphia(final MongoClient mongoClient) {
        this.mongoClient = mongoClient;

        mapper = new Mapper(mongoClient.getMongoClientOptions().getCodecRegistry());
    }

    /**
     * Maps a set of classes
     *
     * @param classes the classes to map
     */
    public void map(final Class... classes) {
        mapper.map(classes);
    }

    /**
     * Creates a Morphia instance with the given Mapper and class set.
     *
     * @param classesToMap the classes to map
     */
    public void map(final Set<Class> classesToMap) {
        mapper.map(classesToMap);
    }

    /**
     * Tries to map all classes in the package specified. Fails if one of the classes is not valid for mapping.
     *
     * @param packageName the name of the package to process
     */
    public void mapPackage(final String packageName) {
        mapper.mapPackage(packageName);
    }

    /**
     * Maps all the classes found in the package to which the given class belongs.
     *
     * @param clazz the class to use when trying to find others to map
     */
    public void mapPackageFromClass(final Class clazz) {
        mapper.mapPackageFromClass(clazz);
    }

    /**
     * It is best to use a Mongo singleton instance here.
     *
     * @param dbName the name of the database
     * @return a Datastore that you can use to interact with MongoDB
     */
    public Datastore createDatastore(final String dbName) {
        return new DatastoreImpl(mongoClient, mapper, dbName);
    }

    /**
     * @return the mapper used by this instance of Morphia
     */
    public synchronized Mapper getMapper() {
        return mapper;
    }

}
