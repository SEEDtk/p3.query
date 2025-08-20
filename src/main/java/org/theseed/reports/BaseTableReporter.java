package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This is the base class for table reporters. The command processor specifies an output file (which may be NULL
 * if STDOUT is allowed) and then a list of column headers. For each row of the report, the command processor
 * must pass in a list of the fields.
 * 
 */
public abstract class BaseTableReporter implements AutoCloseable {

    // FIELDS
    /** pattern for matching numbers */
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+");

    /**
     * This interface must be supported by the controlling command processor.
     */
    public static interface IParms {

        /**
         * @return the ID column spec for FASTA reports
         */
        String getIdColIdx();

        /**
         * @return the sequence column spec for FASTA reports
         */
        String getSeqColIdx();

        /**
         * @return the comment column specs (comma-delimited) for FASTA reports
         */
        String getCommentColIdxs();
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
        /** a FASTA file */
        FASTA {
            @Override
            public BaseTableReporter createReporter(IParms processor, File file) throws IOException {
                return new FastaTableReporter(processor, file);
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

    /**
     * Compute a column index from a column index or name. The index is 1-based. A zero index indicates
     * the last column, and a negative index counts from the end (so -1 is second-to-last). If the
     * column header contains a period, the column name can match anything after the period. If the
     * column specification is invalid, an IO exception is thrown.
     * 
     * @param headers       list of column headers
     * @param colSpec       index (1-based) or name of desired column
     * 
     * @return the numeric array index of the desired column
     * 
     * @throws IOException
     */
    public static int findColumn(List<String> headers, String colSpec) throws IOException {
        int retVal;
        if (NUMBER_PATTERN.matcher(colSpec).matches()) {
            // This is a numeric index. If it's zero or negative, we count from the end.
            int index = Integer.parseInt(colSpec);
            if (index <= 0)
                retVal = headers.size() + index - 1;
            else
                retVal = index - 1; // Convert to zero-based
            // Validate the column index.
            if (retVal < 0 || retVal >= headers.size())
                throw new IOException("Out-of-range column specification: " + colSpec);
        } else {
            // Here we have a column name. We need to search for a match.
            retVal = -1;
            final int colSpecLength = colSpec.length();
            final int n = headers.size();
            String matchString = colSpec.toLowerCase();
            String periodString = "." + matchString;
            for (int i = 0; i < n && retVal < 0; i++) {
                String header = headers.get(i).toLowerCase();
                if (header.length() == colSpecLength) {
                    // Try for an exact match.
                    if (header.equals(matchString))
                        retVal = i;
                } else if (header.length() > colSpecLength) {
                    // Try for a suffix match.
                    if (header.endsWith(periodString))
                        retVal = i;
                }
            }
            if (retVal < 0)
                throw new IOException("Invalid column specification: " + colSpec);
        }
        return retVal;
    }

}
