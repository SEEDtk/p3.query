package org.theseed.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.P3CursorConnection;
import org.theseed.p3api.SolrFilter;

/**
 * This is the base class for BV-BRC query processors. It handles specification of the target table,
 * the output format, the field lists, and the filters.
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
public abstract class BasicQueryProcessor extends BaseBvbrcProcessor {

    // FIELDS
    /** formatted output field list */
    private String fieldList;
    /** list of filters to use */
    private List<SolrFilter> filters;

    // COMMAND-LINE OPTIONS

    /** maximum number of results to return */
    @Option(name = "--limit", metaVar = "100",usage = "maximum number of results to return")
    private int maxResults;

    /** equality filter */
    @Option(name = "--eq", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a value to match")
    private List<String> eqFilters;
    
    /** inequality filter */
    @Option(name = "--ne", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a value not to match")
    private List<String> neFilters;

    /** less-than-or-equal filter */
    @Option(name = "--le", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a number the field value must be less than or equal to")
    private List<String> leFilters;

    /** greater-than-or-equal filter */
    @Option(name = "--ge", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a number the field value must be greater than or equal to")
    private List<String> geFilters;

    /** less-than filter */
    @Option(name = "--lt", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a number the field value must be less than")
    private List<String> ltFilters;

    /** greater-than filter */
    @Option(name = "--gt", metaVar = "<fieldName>,<value>", usage = "name of a table field followed by a number the field value must be greater than")
    private List<String> gtFilters;

    /** one-of filter */
    @Option(name = "--in", metaVar = "<fieldName>,<value1>,<value2>,...", usage = "name of a table field followed by a list of values of which at least one must match")
    private List<String> inFilters;

    /** list of fields to return */
    @Argument(index = 1, metaVar = "outField1 outField2 ...", usage = "names of the fields to return", required = true)
    private List<String> outFields;

    // METHODS

    @Override
    final protected void setBvbrcDefaults() {
        this.maxResults = P3CursorConnection.MAX_LIMIT;
        // Set up the list-value parameters.
        this.eqFilters = new ArrayList<>();
        this.neFilters = new ArrayList<>();
        this.leFilters = new ArrayList<>();
        this.geFilters = new ArrayList<>();
        this.ltFilters = new ArrayList<>();
        this.gtFilters = new ArrayList<>();
        this.inFilters = new ArrayList<>();
        this.outFields = new ArrayList<>();
        // Allow the subclass to set defaults.
        this.setQueryDefaults();
    }

    /**
     * Specify the default parameters for the subclass.
     */
    protected abstract void setQueryDefaults();

    @Override
    final protected void validateBvbrcParms() throws IOException, ParseFailureException {
        // We have access to the BV-BRC API and the data map is set up. We need to compile all the
        // filters.
        this.filters = new ArrayList<>();
        this.eqFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> SolrFilter.EQ(x[0], x[1]))));
        this.neFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> SolrFilter.NE(x[0], x[1]))));
        this.leFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> new SolrFilter.Rel(x[0], true, "*", x[1]))));
        this.ltFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> new SolrFilter.Rel(x[0], false, "*", x[1]))));
        this.geFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> new SolrFilter.Rel(x[0], true, x[1], "*"))));
        this.gtFilters.stream().forEach(f -> this.filters.add(processStringFilter(f, x -> new SolrFilter.Rel(x[0], false, x[1], "*"))));
        // The "in" filter is special, because it has multiple values.
        for (String inFilter : this.inFilters) {
            String[] pieces = StringUtils.split(inFilter, ',');
            if (pieces.length < 2) throw new IllegalArgumentException("Invalid filter format: " + inFilter);
            this.filters.add(SolrFilter.IN(pieces[0], Arrays.copyOfRange(pieces, 1, pieces.length)));
        }
        log.info("{} filters compiled.", this.filters.size());
        // Check the limit.
        if (this.maxResults < 1)
            throw new ParseFailureException("Result limit must be positive.");
        else if (this.maxResults > P3CursorConnection.MAX_LIMIT)
            throw new ParseFailureException("Result limit is too large. The maximum is " + P3CursorConnection.MAX_LIMIT);
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

    /**
     * Convert a string filter parameter to a single SolrFilter object.
     * 
     * @param filter    the incoming filter string
     * @param function  function for converting the string pieces to a filter
     * 
     * @return the corresponding SolrFilter object
     */
    private SolrFilter processStringFilter(String filter, Function<String[], SolrFilter> function) {
        String[] pieces = StringUtils.split(filter, ',');
        if (pieces.length != 2) throw new IllegalArgumentException("Invalid filter format: " + filter);
        return function.apply(pieces);
    }

    @Override
    final protected void runBvbrcCommand(CursorConnection p3, String tableName) throws Exception {
        // Run the query and produce the output.
        this.runQuery(p3, tableName, this.fieldList, this.filters, this.maxResults);
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
    protected abstract void runQuery(CursorConnection p3, String table, String fieldString, List<SolrFilter> queryFilters, int limit) throws Exception;

    /**
     * This is a convenience method so the subclass does not have to re-split the output field names.
     * 
     * @return the list of field names
     */
    protected List<String> getFieldNames() {
        return this.outFields;
    }

}
