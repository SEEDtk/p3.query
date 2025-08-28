package org.theseed.utils;

import java.io.IOException;

import org.kohsuke.args4j.Argument;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.BvbrcDataMap;
import org.theseed.p3api.CursorConnection;

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
public abstract class BaseTableProcessor extends BaseCursorProcessor {

    // FIELDS
    /** table descriptor for the target table */
    private BvbrcDataMap.Table table;

    // COMMAND-LINE OPTIONS

    /** name of the table of interest */
    @Argument(index = 0, metaVar = "tableName", usage = "name of the table to query", required = true)
    private String tableName;


    // METHODS

    @Override
    final protected void setCursorDefaults() {
        // Allow the subclass to set defaults.
        this.setBvbrcDefaults();
    }

    /**
     * Specify the default parameters for the subclass.
     */
    protected abstract void setBvbrcDefaults();

    @Override
    final protected void validateCursorParms(CursorConnection p3) throws IOException, ParseFailureException {
        // Insure the table name is valid.
        this.table = this.getDataMap().getTable(this.tableName);
        if (this.table == null) 
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
    final protected void runCursorCommand(CursorConnection p3) throws Exception {
        // Run the query and produce the output.
        this.runBvbrcCommand(p3, this.tableName);
    }

    /**
     * Run the appropriate query to get the desired results.
     * 
     * @param p3            connection for processing database queries
     * @param table         the name of the table to query
     * 
     * @throws Exception 
     */
    protected abstract void runBvbrcCommand(CursorConnection p3, String table) throws Exception;

    /**
     * @return the user-friendly name of the target table's key field
     */
    protected String getKeyName() {
        return this.table.getKeyField();
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
        return this.table;
    }

}
