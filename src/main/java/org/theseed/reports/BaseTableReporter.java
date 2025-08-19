package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * This is the base class for table reporters. The command processor specifies an output file (which may be NULL
 * if STDOUT is allowed) and then a list of column headers. For each row of the report, the command processor
 * must pass in a list of the fields.
 * 
 */
public abstract class BaseTableReporter implements AutoCloseable {

    /**
     * This interface must be supported by the controlling command processor.
     */
    public static interface IParms {

    }

    /**
     * This enum specifies the type of report.
     */
    public static enum Type {
        /** a normal, tab-delimited report */
        TAB {
            @Override
            public BaseTableReporter createReporter(IParms processor, File file) throws IOException{
                return new TabTableReporter(processor, file);
            }

            @Override
            public boolean supportsStdOut() {
                return true;
            }

        },
        /** a JSON report */
        JSON {
            @Override
            public BaseTableReporter createReporter(IParms processor, File file) throws IOException {
                return new JsonTableReporter(processor, file);
            }

            @Override
            public boolean supportsStdOut() {
                return true;
            }
        };

        /**
         * Create a report writer of this type.
         * 
         * @param processor     the controlling command processor
         * @param file          the output file (or NULL if the output should be to STDOUT)
         * 
         * @return the desired report writer
         * 
         * @throws IOException
         */
        public abstract BaseTableReporter createReporter(IParms processor, File file) throws IOException;

        /**
         * @return TRUE if this writer can write to the standard output
         */
        public abstract boolean supportsStdOut();


    }

    /**
     * Specify the list of header fields.
     * 
     * @param colHeaders    the list of column headers, in order
     */
    public abstract void setHeaders(List<String> colHeaders);

    /**
     * Write a row of data to the report.
     * 
     * @param fields       the list of field values for the row
     */
    public abstract void writeRow(List<String> fields);

    /**
     * Finish the report.
     */
    public abstract void finish();

}
