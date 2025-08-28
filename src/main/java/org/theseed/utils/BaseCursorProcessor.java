package org.theseed.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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
 * There are no positional parameters at this level of the class hierarchy.
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
public abstract class BaseCursorProcessor extends BaseProcessor {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(BaseCursorProcessor.class);
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
        this.setCursorDefaults();
    }

    /**
     * Specify the default parameters for the subclass.
     */
    protected abstract void setCursorDefaults();

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
        // Allow the subclass to set additional parameters.
        this.validateCursorParms(this.p3);
    }

    /**
     * Validate and save the subclass parameters.
     * 
     * @param p3    the cursor connection to validate
     * 
     * @throws IOException 
     * @throws ParseFailureException 
     */
    protected abstract void validateCursorParms(CursorConnection p3) throws ParseFailureException, IOException;

    @Override
    final protected void runCommand() throws Exception {
        // Run the query and produce the output.
        this.runCursorCommand(this.p3);
    }

    /**
     * Run the appropriate query to get the desired results.
     * 
     * @param p3            connection for processing database queries
     * 
     * @throws Exception 
     */
    protected abstract void runCursorCommand(CursorConnection p3) throws Exception;

    /**
     * @return the BV-BRC data map
     */
    protected BvbrcDataMap getDataMap() {
        return this.dataMap;
    }

}
