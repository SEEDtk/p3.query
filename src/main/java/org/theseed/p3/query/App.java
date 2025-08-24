package org.theseed.p3.query;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;
import org.theseed.basic.ICommand;

/**
 * Commands for BV-BRC query utilities
 *
 * list		process an input file containing key values and output the query results
 * get      process a single query
 * fields   list all available fields for a table
 * pipe     process a get query that feeds into a list query
 *
 */
public class App {

    /** static array containing command names and comments */
    protected static final String[] COMMANDS = new String[] {
             "list", "process an input file containing key values and output the query results",
             "get", "process a single query",
             "fields", "list all available fields for a table",
             "pipe", "process a get query that feeds into a list query"
    };

    public static void main( String[] args ) {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        ICommand processor;
        // Determine the command to process.
        switch (command) {

            case "list" -> processor = new QueryListProcessor();

            case "get" -> processor = new QueryGetProcessor();

            case "fields" -> processor = new FieldListProcessor();

            case "pipe" -> processor = new QueryPipeProcessor();
            
            case "-h", "--help" -> processor = null;
            default -> throw new RuntimeException("Invalid command " + command + ".");
        }
        if (processor == null)
            BaseProcessor.showCommands(COMMANDS);
        else {
            processor.parseCommand(newArgs);
            processor.run();
        }
    }
}