package org.theseed.p3.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ICommand;
import org.theseed.basic.ParseFailureException;

/**
 * The query pipe runs a GET query and passes the results to a LIST query. Thus, it implements the most common
 * type of relationship crossing when querying the database.
 * 
 * The queries are run in separate threads, each with its own CursorConnection. A SynchronousQueue is used
 * to pass the result batches from the GET query to the LIST query.
 * 
 * This command has an unusual setup because it requires both a GET and a LIST query to be specified in the
 * command line. This means we have two sets of filters, two output field lists, and two table names.
 * 
 * The GET thread's processing class inherits BaseQueryProcessor. The LIST thread's processing class
 * inherits BaseQueryTableProcessor. We parse our command-line parameters and options and separate them
 * into two sets, depending on whether they are before or after a special "==" divider. The parameters
 * before are passed to the GET thread, and the parameters after are passed to the LIST thread. The
 * exceptions are the "-v"/"--verbose", "--messages", and "--map", which are passed to both as a convenience.
 *
 * The "-h"/"--help" options short-circuit the entire process.
 *
 */
public class QueryPipeProcessor implements ICommand {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(QueryPipeProcessor.class);
    /** thread for executing the GET query */
    private GetPipeProcessor getRunner;
    /** thread for executing the LIST query */
    private ListPipeProcessor listRunner;
    /** parameters for GET thread */
    private final List<String> getParams = new ArrayList<>();
    /** parameters for LIST thread */
    private final List<String> listParams = new ArrayList<>();
    /** map of special options to actions */
    private static final Set<String> BOTH_OPTIONS = Set.of(
            "-v",
            "--verbose",
            "--map",
            "--messages"
           );

    @Override
    public void parseCommand(String[] args) {
        // Create the two handlers.
        this.getRunner = new GetPipeProcessor();
        this.listRunner = new ListPipeProcessor();
        // If an error occurs, we want to catch it and print usage.
        try {
            // Loop through the parameters. Start us in GET mode.
            int i = 0;
            List<String> target = this.getParams;
            while (i < args.length) {
                String arg = args[i];
                if (arg.equals("-h") || arg.equals("--help")) {
                    // Here we short-circuit everything and print usage.
                    this.printUsageAndExit(0);
                } else if (arg.equals("==")) {
                    // Switch to LIST mode, but complain if this is our second arrow.
                    if (target == this.listParams)
                        throw new IllegalArgumentException("Only one '==' allowed in pipe command.");
                    target = this.listParams;
                } else if (BOTH_OPTIONS.contains(arg)) {
                    // These options apply to both queries. The option and its value are added to both parameter lists.
                    int i1 = i + 1;
                    if (i1 >= args.length)
                        throw new ParseFailureException("Missing value for option " + arg + ".");
                    this.getParams.add(arg);
                    this.getParams.add(args[i1]);
                    this.listParams.add(arg);
                    this.listParams.add(args[i1]);
                    // Insure we skip over the value.
                    i = i1;
                } else {
                    // Otherwise, add this parameter to the current target.
                    target.add(arg);
                }
                i++;
            }
            // Now the two command lines are set up. Do the command-line parsing for both commands.
            this.getRunner.parseCommandLine(this.getParams.toArray(String[]::new));
            this.listRunner.parseCommandLine(this.listParams.toArray(String[]::new));
        } catch (CmdLineException | ParseFailureException e) {
            System.err.println(e.toString());
            this.printUsageAndExit(99);
        } catch (IOException e) {
            System.err.println("PARAMETER ERROR: " + e.toString());
            System.exit(99);
        }
    }

    /**
     * Print the usage message and exit with the specified status code.
     * 
     * @param code  status code for the exit
     */
    private void printUsageAndExit(int code) {
        // We display a quick message, then display the usages of the two parameter parts.
        System.err.println("<get-parameters> == <list-parameters>");
        System.err.println("NOTE: the following options are automatically passed to both: " + StringUtils.join(BOTH_OPTIONS, ' '));
        System.err.println("USAGE for <get-parameters>");
        this.getRunner.printUsage();
        System.err.println("USAGE for <list-parameters>");
        this.listRunner.printUsage();
        System.exit(code);
    }

    @Override
    public void run() {
        // Create the threads.
        Thread getThread = new Thread(this.getRunner, "GetThread");
        Thread listThread = new Thread(this.listRunner, "ListThread");
        log.info("GET and LIST threads created.");
        // Set up the messaging queue.
        SynchronousQueue<List<String>> queue = new SynchronousQueue<>();
        this.getRunner.setResultQueue(queue);
        this.listRunner.setInputQueue(queue);
        // Start the threads.
        getThread.start();
        listThread.start();
        // Wait for the threads to stop.
        try {
            getThread.join();
            listThread.join();
        } catch (InterruptedException e) {
            log.error("Thread interrupted", e);
        }
    }
    
}
