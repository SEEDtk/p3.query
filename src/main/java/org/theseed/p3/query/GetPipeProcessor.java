package org.theseed.p3.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.SolrFilter;
import org.theseed.utils.BaseQueryTableProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This class processes the GET part of a query pipeline. It will run a single query and pass batches of results to the LIST processor.
 * It accepts command-line parameters selected by the master QueryPipeProcessor to configure the query. 
 * 
 * The positional parameters are the name of the table to query and the name of the field to feed to the list processor.
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -b   number of records to pass to the LIST processor in each batch
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --limit    maximum number of results to return (default 1 billion)
 * 
 * The following parameters are used to filter the query results. Each can be repeated multiple
 * times, and the field names and values must be comma-delimited.
 * 
 * --eq       the name of a table field followed by a value to match
 * --ne       the name of a table field followed by a value not to match
 * --le       the name of a table field followed by a number that the field value must be less than or equal to
 * --ge       the name of a table field followed by a number that the field value must be greater than or equal to
 * --lt       the name of a table field followed by a number that the field value must be less than
 * --gt       the name of a table field followed by a number that the field value must be greater than
 * --in       the name of a table field followed by a list of values of which at least one must match
 * 
 */
public class GetPipeProcessor extends BaseQueryTableProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(GetPipeProcessor.class);
    /** queue for passing query results to the list processor */
    private SynchronousQueue<List<String>> resultQueue;
    /** current batch of query results */
    private List<String> currentBatch;
    /** special terminating batch */
    protected static final List<String> TERMINATING_BATCH = Collections.emptyList();

    // COMMAND-LINE OPTIONS

    /** batch size for queries */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "500", usage = "batch size for queries")
    private int batchSize;

    /** table field to pass to list processor */
    @Argument(index = 0, metaVar = "tableField", usage = "name of the table field to pass to the list processor", required = true)
    private String tableField;

    @Override
    protected void setTableDefaults() {
        this.batchSize = 200;
    }

    @Override
    protected void validateTableParms() throws ParseFailureException, IOException {
        if (this.batchSize < 1)
            throw new ParseFailureException("Batch size must be at least 1.");
        // Create the batch holder.
        this.currentBatch = new ArrayList<>(this.batchSize);
    }

    @Override
    protected void runTable(CursorConnection p3, String table, List<SolrFilter> queryFilters, long limit) throws Exception {
        // Compute the field list for the query. It consists only of the output field.
        long count = p3.getRecords(table, limit, this.tableField, queryFilters, (x -> this.processRecord(x)));
        log.info("{} records found.", count);
        // Process the residual batch.
        if (! this.currentBatch.isEmpty()) {
            this.sendBatch();
        }
        // Terminate the LIST processor by sending an empty list.
        this.resultQueue.put(TERMINATING_BATCH);
    }
        
    /**
     * Process a single record from the query. We will extract the key field and add it to the current
     * batch. If the batch is full, we send it to the result queue.
     * 
     * @param record    record returned by the query.
     */
    private void processRecord(JsonObject record) {
        // Insure there is room in the current batch.
        if (this.currentBatch.size() >= this.batchSize) {
            this.sendBatch();
            // Create a new batch.
            this.currentBatch = new ArrayList<>(this.batchSize);
        }
        // Add the key to the 
        String key = KeyBuffer.getString(record, this.tableField);
        this.currentBatch.add(key);
    }

    /**
     * Send the current batch to the LIST processor.
     */
    private void sendBatch() {
        try {
            this.resultQueue.put(this.currentBatch);
        } catch (InterruptedException e) {
            log.warn("GET processor interrupted while sending batch to LIST processor.");
        }
    }

    /**
     * This method is used to tell the GET processor where to store results. The incoming
     * batch list consist of keys to look for in the LIST table.
     * 
     * @param resultQueue	queue for passing query results to the LIST processor
     */
    protected void setResultQueue(SynchronousQueue<List<String>> resultQueue) {
        this.resultQueue = resultQueue;
    }

}
