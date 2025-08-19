package org.theseed.p3.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.io.TabbedLineReader;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.KeyBuffer;
import org.theseed.p3api.SolrFilter;
import org.theseed.reports.BaseTableReporter;
import org.theseed.utils.BaseQueryTableReportProcessor;
import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This is a query command that queries the database using a list of key values in a file and produces
 * the output as a tab-delimited report. The list of key values is read from a tab-delimited input
 * file, with headers.
 * 
 * The positional parameter is the name of the table to query followed by the names ofhe fields to include
 * in the output.
 * 
 * The table name and all the field names in the output list and the query filters must be user-friendly
 * names according to the selected BV-BRC data map.
 * 
 * Note that for text-type database fields (string_ci in SOLR), the field values are case-insensitive, and
 * matching a word is considered an exact match. The asterisk wild card (*) can be used in these cases,
 * but only in a string without special characters or spaces. These are all limits of the SOLR technology.
 * 
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -o   output file (if not STDOUT)
 * -i   input file (if not STDIN)
 * -b   batch size for queries (default 100)
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --limit    maximum number of results to return (default 1 billion)
 * --format   output format for the report (default TAB)
 * --key      name of the key field in the query table (defaults to primary key)
 * --col      index (1-based) or name of the column from the input file containing the key values (default "1")
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
 */
public class QueryListProcessor extends BaseQueryTableReportProcessor {

    // FIELDS
    /** key column index */
    private int keyColIdx;
    /** input file stream */
    private TabbedLineReader inStream;
    /** number of keys not found */
    private int keysNotFound;

    // COMMAND-LINE OPTIONS

    /** batch size for queries */
    @Option(name = "--batch", aliases = { "-b" }, metaVar = "500", usage = "batch size for queries")
    private int batchSize;

    /** name of the key field in the target table (defaults to the ID field) */
    @Option(name = "--key", metaVar = "name", usage = "name of the key field to use in the target table (default to primary key)")
    private String keyName;

    /** index/name of the key field in the input file */
    @Option(name = "--col", metaVar = "name", usage = "index (1-based) or name of the key field in the input file")
    private String keyCol;

    /** name of the input file containing the keys */
    @Option(name = "--input", aliases = { "-i" }, metaVar = "inFile.tbl", usage = "name of the input file containing the keys")
    private File inFile;

    // METHODS

    @Override
    protected void setQReportDefaults() {
        this.keyName = null;
        this.keyCol = "1";
        this.inFile = null;
    }

    @Override
    protected void validateQReportParms() throws IOException, ParseFailureException {
        // Validate the batch size.
        if (this.batchSize < 2)
            throw new ParseFailureException("Batch size must be greater than 1.");
        // Fix the key field default.
        if (this.keyName == null) this.keyName = this.getKeyName();
        // Set up the input file stream.
        if (this.inFile != null) {
            log.info("Input keys will be taken from the file {}.", this.inFile);
            this.inStream = new TabbedLineReader(this.inFile);
        } else {
            log.info("Input keys will be taken from standard input.");
            this.inStream = new TabbedLineReader(System.in);
        }
        // Compute the key column index.
        this.keyColIdx = this.inStream.findField(this.keyCol);
    }

    @Override
    protected void runQueryReport(BaseTableReporter reporter, CursorConnection p3, String table, String fieldString,
            List<SolrFilter> queryFilters, int limit) throws Exception {
        try {
            // We must first initialize the report. The report contains all the columns from the input file plus
            // all the columns from the target table. We modify the target table column header to include the
            // target table name.
            List<String> headers = new ArrayList<String>();
            Arrays.stream(this.inStream.getLabels()).forEach(x -> headers.add(x));
            String tableName = this.getTableName();
            this.getFieldNames().stream().forEach(x -> headers.add(tableName + "." + x));
            reporter.setHeaders(headers);
            // Insure the key field name is in the field string. It should not be, because the user won't want it
            // to appear twice in the report. If it is, the redundancy will be removed automatically by the
            // query engine.
            String realFieldString;
            if (fieldString.isEmpty())
                realFieldString = this.keyName;
            else
                realFieldString = fieldString + "," + this.keyName;
            // This will track the number of results output. We use this to compute the limit for each sub-query.
            int outCount = 0;
            // Thes are statistics for logging.
            int inCount = 0;
            int batchCount = 0;
            this.keysNotFound = 0;
            // The batch is built in here. We use a linked map so we can unroll it in input order.
            Map<String, List<String>> lineMap = new LinkedHashMap<String, List<String>>();
            // We use this iterator to loop through the input lines.
            Iterator<TabbedLineReader.Line> lineIter = this.inStream.iterator();
            while (lineIter.hasNext() && outCount < limit) {
                // Get the next input line.
                var line = lineIter.next();
                inCount++;
                // Insure there is room for the line in the batch.
                if (lineMap.size() >= this.batchSize) {
                    // Process the current batch.
                    batchCount++;
                    log.info("Processing batch {} with {} lines.", batchCount, lineMap.size());
                    outCount += this.processQueryBatch(lineMap, reporter, p3, table, realFieldString, queryFilters, limit - outCount);
                    // Clear the line map for the next batch.
                    lineMap.clear();
                }
                // Add this line to the batch. Note that if we have a duplicate key, it will be overwritten.
                List<String> fields = Arrays.asList(line.getFields());
                lineMap.put(line.get(this.keyColIdx), fields);
            }
            // Process the residual batch.
            if (! lineMap.isEmpty() && outCount < limit) {
                log.info("Processing final batch with {} lines.", lineMap.size());
                outCount +=this.processQueryBatch(lineMap, reporter, p3, table, realFieldString, queryFilters, limit - outCount);
            }
            log.info("{} input lines processed, {} output lines written, {} keys not found.", inCount, outCount, this.keysNotFound);
        } finally {
            this.inStream.close();
        }
    }

    /**
     * Process a batch of queries and return the number of lines output.
     * 
     * @param lineMap       map of database keys to input lines  
     * @param reporter      report writer
     * @param p3            database connection
     * @param table         target table name
     * @param fieldString   fields to include in the query
     * @param filters       query filters
     * @param limit         maximum number of results to return
     *
     * @return the number of lines output
     * 
     * @throws IOException 
     */
    private int processQueryBatch(Map<String, List<String>> lineMap, BaseTableReporter reporter, CursorConnection p3,
            String table, String fieldString, List<SolrFilter> filters, int limit) throws IOException {
        // This will track the number of lines output.
        int retVal = 0;
        // First, we must run the query. We use the keyset from the line map.
        List<JsonObject> results = p3.getRecords(table, limit, lineMap.size(), this.keyName, lineMap.keySet(),
                fieldString, filters);
        log.info("{} records found for {} keys.", results.size(), lineMap.size());
        // Sort the results by the key name. For each result, we extract the output fields into a list. This will
        // form the second part of each output line. This will hold the sorted results.
        Map<String, List<List<String>>> resultMap = new HashMap<String, List<List<String>>>(lineMap.size() * 4 / 3 + 1);
        // We need the field names for the output fields.
        List<String> fieldNames = this.getFieldNames();
        for (JsonObject record : results) {
            // Get the key value from this record.
            String keyValue = KeyBuffer.getString(record, this.keyName);
            // Get the output fields for this record.
            List<String> row = new ArrayList<String>(fieldNames.size());
            for (String fieldName : fieldNames)
                row.add(KeyBuffer.getString(record, fieldName));
            // Put the formed line in the sort map.
            resultMap.computeIfAbsent(keyValue, k -> new ArrayList<>()).add(row);
        }
        // Now we loop through the line map, writing output records for each key.
        for (var lineEntry : lineMap.entrySet()) {
            // Get the key value and the input fields.
            String keyValue = lineEntry.getKey();
            List<String> inputFields = lineEntry.getValue();
            // Get the output lines for this key. Not having any output lines is an acceptable result.
            List<List<String>> outputLines = resultMap.get(keyValue);
            if (outputLines == null) {
                // We have no output lines for this key, so we increment the not found counter.
                this.keysNotFound++;
            } else {
                // We have at least one output line. We write the input fields first, then the output fields.
                for (List<String> outputRow : outputLines) {
                    List<String> fullRow = new ArrayList<>(inputFields);
                    fullRow.addAll(outputRow);
                    reporter.writeRow(fullRow);
                    retVal++;
                }
            }
        }
        return retVal;
    }

}
