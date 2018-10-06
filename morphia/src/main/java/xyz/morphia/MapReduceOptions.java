/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xyz.morphia;

import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.MapReduceAction;
import org.bson.Document;
import xyz.morphia.query.Query;
import xyz.morphia.utils.Assert;

import java.util.concurrent.TimeUnit;

/**
 * This defines options that can be applied to a mapreduce job
 *
 * @param <T> the type of the output
 * @since 1.3
 */
public class MapReduceOptions<T> {
    private String outputDB;
    private String outputCollection;
    private String map;
    private String reduce;
    private OutputType outputType;
    private Query<?> query;
    private String finalize;
    private int limit;
    private long maxTimeMS;
    private Document scope;
    private Boolean jsMode;
    private Boolean verbose;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private Class<T> resultType;

    /**
     * Sets whether to bypass document validation.
     *
     * @param bypassDocumentValidation whether to bypass document validation, or null if unspecified
     * @return this
     * @mongodb.server.release 3.2
     */
    public MapReduceOptions<T> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Sets the collation options
     *
     * @param collation the collation options
     * @return this
     * @mongodb.server.release 3.4
     */
    public MapReduceOptions<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Sets the Finalize JS Function
     *
     * @param finalize The finalize function (as a JS String)
     * @return this
     */
    public MapReduceOptions<T> finalize(final String finalize) {
        this.finalize = finalize;
        return this;
    }

    /**
     * Sets the (optional) JavaScript Mode
     *
     * @param jsMode Specifies whether to convert intermediate data into BSON format between the execution of the map and reduce functions
     * @return this
     */
    public MapReduceOptions<T> jsMode(final Boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    /**
     * Sets the (optional) limit on input
     *
     * @param limit The limit specification object
     * @return this
     */
    public MapReduceOptions<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Set the JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     *
     * @param map the JavaScript function
     * @return this
     */
    public MapReduceOptions<T> map(final String map) {
        Assert.parametersNotNull("map", map);
        Assert.parameterNotEmpty("map", map);
        this.map = map;
        return this;
    }

    /**
     * Sets the max execution time for this command, in the given time unit.
     *
     * @param maxTimeMS the maximum execution time in milliseconds. A non-zero value requires a server version &gt;= 2.6
     * @return this
     * @mongodb.server.release 2.6
     */
    public MapReduceOptions<T> maxTimeMS(final long maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    /**
     * Sets the output collection for the job
     *
     * @param name the collection name
     * @return this
     */
    public MapReduceOptions<T> outputCollection(final String name) {
        this.outputCollection = name;
        return this;
    }

    /**
     * Sets the (optional) database name where the output collection should reside
     *
     * @param outputDB the name of the database to send the Map Reduce output to
     * @return this
     */
    public MapReduceOptions<T> outputDB(final String outputDB) {
        this.outputDB = outputDB;
        return this;
    }

    /**
     * Sets the output type of the job
     *
     * @param outputType the output type
     * @return this
     */
    public MapReduceOptions<T> outputType(final OutputType outputType) {
        this.outputType = outputType;
        return this;
    }

    /**
     * Sets the query defining the input for the job.  Must not be null.
     *
     * @param query the query to use
     * @return this
     */
    public MapReduceOptions<T> query(final Query<?> query) {
        Assert.parametersNotNull("query", query);
        this.query = query;
        return this;
    }

    /**
     * Sets the JavaScript function that "reduces" to a single object all the values associated with a particular key.
     *
     * @param reduce the javascript function
     * @return this
     */
    public MapReduceOptions<T> reduce(final String reduce) {
        Assert.parametersNotNull("reduce", reduce);
        Assert.parameterNotEmpty("reduce", reduce);
        this.reduce = reduce;
        return this;
    }

    /**
     * Sets the result type of the job
     *
     * @param resultType the type
     * @return this
     */
    public MapReduceOptions<T> resultType(final Class<T> resultType) {
        this.resultType = resultType;
        return this;
    }

    /**
     * Sets the (optional) JavaScript scope
     *
     * @param scope The JavaScript scope
     * @return this
     */
    public MapReduceOptions<T> scope(final Document scope) {
        this.scope = scope;
        return this;
    }

    /**
     * Sets the verbosity of the MapReduce job, defaults to 'true'
     *
     * @param verbose The verbosity level.
     * @return this
     */
    public MapReduceOptions<T> verbose(final Boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    OutputType getOutputType() {
        return outputType;
    }

    Query<?> getQuery() {
        return query;
    }

    Class<T> getResultType() {
        return resultType;
    }

    /**
     * @return the map function
     */
    public String getMap() {
        return map;
    }

    /**
     * @return the reduce function
     */
    public String getReduce() {
        return reduce;
    }

    @SuppressWarnings("deprecation")
    MapReduceIterable<T> apply(final MapReduceIterable<T> iterable) {

        if (outputCollection != null) {
            iterable.collectionName(outputCollection);
        }

        switch (outputType) {
            case REPLACE:
                iterable.action(MapReduceAction.REPLACE);
                break;
            case MERGE:
                iterable.action(MapReduceAction.MERGE);
                break;
            case REDUCE:
                iterable.action(MapReduceAction.REDUCE);
                break;
            case INLINE:
                break;
            default:
                break;
        }
        iterable.bypassDocumentValidation(bypassDocumentValidation);
        iterable.collation(collation);
        iterable.filter(query.getQueryDocument());
        iterable.finalizeFunction(finalize);
        iterable.jsMode(jsMode);
        iterable.limit(limit);
        iterable.maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
        iterable.databaseName(outputDB);
        iterable.scope(scope);
        iterable.sort(query.getSortDocument());
        iterable.verbose(verbose);

        return iterable;
    }
}





















