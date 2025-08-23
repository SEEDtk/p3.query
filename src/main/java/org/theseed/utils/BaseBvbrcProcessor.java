package org.theseed.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.BvbrcDataMap;
import org.theseed.p3api.CursorConnection;

import com.github.cliftonlabs.json_simple.JsonException;

/**
 * This is the base class for BV-BRC database processors. It handles connection to the
 * BV-BRC database and provides common parameters for mapping.
 *
 * The positional parameter is the name of the table to query.
 *
 * The table name must be a user-friendly name according to the selected BV-BRC data map.
 * 
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --chunk    the size of the data chunks for each request (default is 25000)
 */
public abstract class BaseBvbrcProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(BaseBvbrcProcessor.class);
    /** BV-BRC cursor connection */
    private CursorConnection p3;
    /** BV-BRC data map */
    private BvbrcDataMap dataMap;

    // COMMAND-LINE OPTIONS

    /** name of the JSON file for the BVBRC data map to use */
    @Option(name = "--map", metaVar = "dataMap.json", usage = "name of the JSON file for the BVBRC data map to use")
    private File mapFile;

    /** number of seconds to wait between status messages on the log */
    @Option(name = "--messages", metaVar = "10", usage = "number of seconds between status messages (0 = off)")
    private int statusMessageInterval;

    /** name of the table of interest */
    @Argument(index = 0, metaVar = "tableName", usage = "name of the table to query", required = true)
    private String tableName;

    /** size of the data chunks to process */
    @Option(name = "--chunk", metaVar = "200", usage = "size of the data chunks to retrieve for each request")
    private int chunkSize;

    // METHODS

    @Override
    final protected void setDefaults() {
        this.statusMessageInterval = 10;
        this.chunkSize = 25000;
        // If no map file is specified, we use the default map.
        this.mapFile = null;
        // Allow the subclass to set defaults.
        this.setBvbrcDefaults();
    }

    /**
     * Specify the default parameters for the subclass.
     */
    protected abstract void setBvbrcDefaults();

    @Override
    final protected void validateParms() throws IOException, ParseFailureException {
        // Get the BV-BRC data map.
        if (this.mapFile == null) {
            log.info("Using default BV-BRC data map.");
            this.dataMap = BvbrcDataMap.DEFAULT_DATA_MAP;
        } else if (! this.mapFile.canRead())
            throw new FileNotFoundException("BV-BRC data map file not found or unreadable: " + this.mapFile.getAbsolutePath());
        else {
            log.info("Using BV-BRC data map from file {}.", this.mapFile);
            try {
                this.dataMap = BvbrcDataMap.load(this.mapFile);
            } catch (JsonException e) {
                throw new IOException("Error loading BV-BRC data map file: " + e.toString());
            }
        }
        // Verify the chunk size.
        if (this.chunkSize < 1)
            throw new ParseFailureException("Chunk size must be a positive integer.");
        if (this.chunkSize > 25000)
            throw new ParseFailureException("Chunk size must not exceed 25000.");
        // Connect to the database.
        this.p3 = new CursorConnection(dataMap);
        this.p3.setChunkSize(this.chunkSize);
        this.p3.setMessageGap(this.statusMessageInterval);
        // Insure the table name is valid.
        BvbrcDataMap.Table table = this.dataMap.getTable(this.tableName);
        if (table == null) 
            throw new ParseFailureException("Unknown table: " + this.tableName);
        // Allow the subclass to set additional parameters.
        this.validateBvbrcParms();
    }

    /**
     * Validate and save the subclass parameters.
     * 
     * @throws IOException 
     * @throws ParseFailureException 
     */
    protected abstract void validateBvbrcParms() throws ParseFailureException, IOException;

    @Override
    final protected void runCommand() throws Exception {
        // Run the query and produce the output.
        this.runBvbrcCommand(this.p3, this.tableName);
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
    protected abstract void runBvbrcCommand(CursorConnection p3, String table) throws Exception;

    /**
     * @return the user-friendly name of the target table's key field
     */
    protected String getKeyName() {
        String retVal;
        try {
            retVal = this.dataMap.getTable(this.tableName).getKeyField();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return retVal;
    }

    /**
     * @return the target table name (user-friendly)
     */
    protected String getTableName() {
        return this.tableName;
    }

    /**
     * @return the data map for the target table
     */
    protected BvbrcDataMap.Table getTableMap() {
        BvbrcDataMap.Table retVal;
        try {
            retVal = this.dataMap.getTable(this.tableName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return retVal;
    }

}
