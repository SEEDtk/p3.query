package org.theseed.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.SolrFilter;
import org.theseed.reports.BaseTableReporter;

/**
 * This is a subclass of the query processor that writes tabular output.
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
 * -o   output file for report (if not STDOUT)
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --limit    maximum number of results to return (default 1 billion)
 * --format   the output format (default is TAB, for tab-delimited text)
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
 * --id       the ID column spec for FASTA reports (default "1")
 * --seq      the sequence column spec for FASTA reports (default "0")
 * --comment  the comment column specs (comma-delimited) for FASTA reports (default "")
 */
public abstract class BaseQueryTableReportProcessor extends BasicQueryProcessor implements BaseTableReporter.IParms {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(BaseQueryTableReportProcessor.class);
    
    // COMMAND-LINE OPTIONS

    /** output file for report (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "outFile", usage = "output file for report (if not STDOUT)")
    private File outputFile;

    /** output format */
    @Option(name = "--format", usage = "the output format")
    private BaseTableReporter.Type outputFormat;

    /** ID column spec for FASTA reports */
    @Option(name = "--id", usage = "index (1-based) or name of the ID column for FASTA reports")
    private String idColSpec;

    /** sequence column spec for FASTA reports */
    @Option(name = "--seq", usage = "index (1-based) or name of the sequence column for FASTA reports")
    private String seqColSpec;

    /** comment column specs for FASTA reports */
    @Option(name = "--comment", usage = "indices (1-based) or names of the comment columns for FASTA reports")
    private String commentColSpecs;

    // METHODS

    @Override
    final protected void setQueryDefaults() {
        this.outputFile = null;
        this.outputFormat = BaseTableReporter.Type.TAB;
        // Set the column specs for FASTA reports.
        this.idColSpec = "1";
        this.seqColSpec = "0";
        this.commentColSpecs = "";
        // Set the subclass defaults.
        this.setQReportDefaults();
    }

    /**
     * Specify the command-line option defaults.
     */
    protected abstract void setQReportDefaults();

    @Override
    final protected void validateQueryParms() throws ParseFailureException, IOException {
        // Verify that the output file is valid.
        if (this.outputFile == null) {
            if (! this.outputFormat.supportsStdOut())
                throw new ParseFailureException("Output format " + this.outputFormat + " does not support standard output.");
            else
                log.info("Report will be written to the standard output.");
        } else {
            if (! this.outputFile.exists()) {
                // Verify we can create a file with this name.
                try (PrintWriter testWriter = new PrintWriter(this.outputFile)) {
                    // If we can open the file, we can write to it.
                    log.info("Report will be written to file {}.", this.outputFile);
                    testWriter.println();
                }
            } else if (! this.outputFile.canWrite())
                throw new IOException("Cannot write to output file " + this.outputFile + ".");
            else
                log.info("Report will be written to file {}.", this.outputFile);
        }
        // Validate the subclass parameters.
        this.validateQReportParms();
    }

    /**
     * Validate and process the command-line parameters.
     * @throws ParseFailureException 
     * @throws IOException 
     */
    protected abstract void validateQReportParms() throws IOException, ParseFailureException;

    @Override
    final protected void runQuery(CursorConnection p3, String table, String fieldString, List<SolrFilter> queryFilters,
            long limit) throws Exception {
        // Create the report writer.
        try (BaseTableReporter reporter = this.outputFormat.createReporter(this, this.outputFile)) {
            // Produce the report.
            this.runQueryReport(reporter, p3, table, fieldString, queryFilters, limit);
        }

    }

    /**
     * Run the desired query and produce the report.
     *
     * @param reporter      the report writer
     * @param p3            the database connection
     * @param table         the table to query
     * @param fieldString   the fields to include in the report
     * @param queryFilters  the filters to apply to the query
     * @param limit         the maximum number of rows to return
     * 
     * @throws Exception
     */
    protected abstract void runQueryReport(BaseTableReporter reporter, CursorConnection p3, String table,
            String fieldString, List<SolrFilter> queryFilters, long limit) throws Exception;

    @Override
    public String getIdColIdx() {
        return this.idColSpec;
    }

    @Override
    public String getSeqColIdx() {
        return this.seqColSpec;
    }

    @Override
    public String getCommentColIdxs() {
        return this.commentColSpecs;
    }

}
