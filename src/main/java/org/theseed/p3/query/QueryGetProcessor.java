package org.theseed.p3.query;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.reports.BaseTableReporter;
import org.theseed.utils.BaseQueryTableReportProcessor;

import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This is a query command that performs a single query against the database and produces a simple tab-delimited report.
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
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --limit    maximum number of results to return (default 1 billion)
 * --format   output format for the report (default TAB)
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
public class QueryGetProcessor extends BaseQueryTableReportProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(QueryGetProcessor.class);
    /** list of output field names, in order */
    private List<String> outputFields;

    // METHODS

    @Override
    protected void setQReportDefaults() {
    }

    @Override
    protected void validateQReportParms() {
    }

    @Override
    protected void runQueryReport(BaseTableReporter reporter, CursorConnection p3, String table, String fieldString,
            List<SolrFilter> queryFilters, long limit) throws Exception {
        // Get the output field list.
        this.outputFields = this.getFieldNames();
        // Set up the output headers.
        reporter.setHeaders(this.outputFields);
        // Run the query, sending each record found to the reporter.
        log.info("Running query on table {} with fields {} and limit {}.", table, fieldString, limit);
        long count = p3.getRecords(table, limit, fieldString, queryFilters, x -> this.writeRowFromRecord(reporter, x));
        log.info("{} records found.", count);
        // Finish the report.
        reporter.finish();
    }

    /**
     * Write the current row to the output.
     *
     * @param reporter  output report writer
     * @param record    database record to write
     */
    private void writeRowFromRecord(BaseTableReporter reporter, JsonObject record) {
        List<Object> row = this.outputFields.stream().map(x -> record.get(x)).toList();
        reporter.writeRow(row);
    }

}
