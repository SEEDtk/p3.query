package org.theseed.p3.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.reports.BaseTableReporter;
import org.theseed.utils.BaseQueryTableReportProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This processor performs the LIST operation for a pipe. It retrieves data from the GET processor,
 * uses it to power queries, and then produces a report. The default report is to the standard
 * output.
 * 
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -o   output file for report (if not STDOUT)
 *
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --limit    maximum number of results to return (default 1 billion)
 * --format   the output format (default is TAB, for tab-delimited text)
 * --key      name of the key field in the query table (defaults to primary key)
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
 * The following parameters modify report output, and are column specs for the output columns. Note that a column spec is 
 * either a numeric index (1-based) or a column name. A zero index means the last column, and a negative index counts from
 * the end (so -1 is second-to-last). A column name is case-insensitive, and if an output column header contains periods,
 * it can match anything after the period.
 *
 * --id       the ID column spec for FASTA and COUNT reports (default "1")
 * --seq      the sequence column spec for FASTA reports (default "0")
 * --comment  the comment column specs (comma-delimited) for FASTA reports (default "")
 * 
 */
public class ListPipeProcessor extends BaseQueryTableReportProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(ListPipeProcessor.class);
    /** queue for getting query results from the GET processor */
    private SynchronousQueue<List<String>> inputQueue;
    /** current batch of query results */
    private List<String> currentBatch;

    // COMMAND-LINE OPTIONS

    /** name of the key field in the target table (defaults to the ID field) */
    @Option(name = "--key", metaVar = "name", usage = "name of the key field to use in the target table (default to primary key)")
    private String keyName;

    @Override
    protected void setQReportDefaults() {
        this.keyName = null;
    }

    @Override
    protected void validateQReportParms() throws IOException, ParseFailureException {
        // Compute the target table key name.
        if (this.keyName == null)
            this.keyName = this.getKeyName();
        log.info("Using {} as key field in {}.", this.keyName, this.getTableName());
    }

    @Override
    protected void runQueryReport(BaseTableReporter reporter, CursorConnection p3, String table, String fieldString,
            List<SolrFilter> queryFilters, long limit) throws Exception {
        // Get the output field names. These are our report column headers.
        List<String> outFields = this.getFieldNames();
        // Create a buffer to hold the output lines.
        List<Object> outputBuffer = new ArrayList<>(outFields.size());
        // Initialize the report processor.
        reporter.setHeaders(outFields);
        // Determine the acceptable number of rows left.
        long rowsLeft = limit;
        // Process batches, terminating on an empty one.
        while ((this.currentBatch = this.inputQueue.take()) != GetPipeProcessor.TERMINATING_BATCH) {
            // Skip this batch if we have exhausted the output limit.
            if (rowsLeft > 0) {
                // Query the current batch.
                log.info("Processing batch of {} records.", this.currentBatch.size());
                List<JsonObject> records = p3.getRecords(table, rowsLeft, this.currentBatch.size(), keyName, currentBatch, fieldString, queryFilters);
                // Update the rows-left count.
                long count = records.size();
                rowsLeft -= count;
                log.info("{} records found in report batch.", count);
                // Now we must write the records to the report. Each record is converted to a list of values
                // corresponding to the output columns.
                for (JsonObject record : records) {
                    outFields.stream().forEach(x -> outputBuffer.add(record.get(x)));
                    reporter.writeRow(outputBuffer);
                    outputBuffer.clear();
                }
            }
        }
        log.info("All batches processed.");
    }

    /**
     * This method is used to tell the LIST processor where to get results.
     */
    protected void setInputQueue(SynchronousQueue<List<String>> inputQueue) {
        this.inputQueue = inputQueue;
    }

}
