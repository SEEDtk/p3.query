package org.theseed.utils;

import java.io.IOException;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.SolrFilter;

/**
 * This is the base class for BV-BRC query processors. It handles specification of the target table,
 * the field lists, and the filters. Thus, it differs from BaseQueryProcessor only in the addition of 
 * an output field list
 *
 * The positional parameter is the name of the table to query.
 *
 * The table name must be a user-friendly name according to the selected BV-BRC data map.
 *
 * Note that for text-type database fields (string_ci in SOLR), the field values are case-insensitive, and
 * matching a word is considered an exact match. The asterisk wild card (*) can be used in these cases,
 * but only in a string without special characters or spaces. These are all limits of the SOLR technology.
 * 
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
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
 */
public abstract class BaseQueryProcessor extends BaseQueryTableProcessor {

    // FIELDS
    /** formatted output field list */
    private String fieldList;

    // COMMAND-LINE OPTIONS

    /** list of fields to return */
    @Argument(index = 1, metaVar = "outField1 outField2 ...", usage = "names of the fields to return", required = true)
    private List<String> outFields;

    // METHODS

    @Override
    final protected void setTableDefaults() {
        // Allow the subclass to set defaults.
        this.setQueryDefaults();
    }

    /**
     * Specify the default parameters for the subclass.
     */
    protected abstract void setQueryDefaults();

    @Override
    final protected void validateTableParms() throws IOException, ParseFailureException {
        // Set up the output field list.
        this.fieldList = String.join(",", this.outFields);
        // Allow the subclass to set additional parameters.
        this.validateQueryParms();
    }

    /**
     * Validate and save the subclass parameters.
     * @throws IOException 
     * @throws ParseFailureException 
     */
    protected abstract void validateQueryParms() throws ParseFailureException, IOException;

    @Override
    final protected void runTable(CursorConnection p3, String table, List<SolrFilter> queryFilters, long limit) throws Exception {
        // Run the query and produce the output.
        this.runQuery(p3, table, this.fieldList, queryFilters, limit);
    }

    /**
     * Run the appropriate query to get the desired results.
     * 
     * @param p3            connection for processing database queries
     * @param report        the report controller
     * @param table         the name of the table to query
     * @param fieldString   the comma-delimited field list
     * @param queryFilters  the list of query filters
     * @param limit         the maximum number of results to return
     * 
     * @throws Exception 
     */
    protected abstract void runQuery(CursorConnection p3, String table, String fieldString, List<SolrFilter> queryFilters, long limit) throws Exception;

    /**
     * This is a convenience method so the subclass does not have to re-split the output field names.
     * 
     * @return the list of field names
     */
    protected List<String> getFieldNames() {
        return this.outFields;
    }

}
