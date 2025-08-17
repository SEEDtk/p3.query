package org.theseed.p3.query;

import java.util.Arrays;

import org.theseed.basic.BaseProcessor;

/**
 * Commands for BV-BRC query utilities
 *
 * list		process an input file containing key values and output the query results
 * get      process a single query
 *
 */
public class App {

    /** static array containing command names and comments */
    protected static final String[] COMMANDS = new String[] {
             "list", "process an input file containing key values and output the query results",
             "get", "process a single query",
    };

    public static void main( String[] args ) {
        // Get the control parameter.
        String command = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);
        BaseProcessor processor;
        // Determine the command to process.
        switch (command) {

            case "list" :
                processor = new QueryListProcessor();
                break;

            case "get" :
                processor = new QueryGetProcessor();
                break;

            case "-h" :
            case "--help" :
                processor = null;
                break;
                
            default :
                throw new RuntimeException("Invalid command " + command + ".");
        }
        if (processor == null)
            BaseProcessor.showCommands(COMMANDS);
        else {
            processor.parseCommand(newArgs);
            processor.run();
        }
    }
}