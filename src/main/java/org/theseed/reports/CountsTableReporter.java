package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountsTableReporter extends BaseTableReporter {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(CountsTableReporter.class);
    /** output print writer */
    private final PrintWriter writer;
    /** index of ID column */
    private int idColIdx;
    /** counts for each ID column value */
    private final Map<String, LongCount> counts;
    /** controlling command processor */
    private final BaseTableReporter.IParms controller;
    /** total for all field values */
    private long totalCount;
    /** total for empty field values */
    private long emptyCount;

    /**
     * This class is a counter that sits in a long integer.
     */
    public static class LongCount {

        /** the long integer count value */
        private long count;

        /**
         * Construct a new long count.
         */
        public LongCount() {
            this.count = 0;
        }

        /**
         * Increment the count.
         */
        public void count() {
            this.count++;
        }

        /**
         * @return the count
         */
        public long getCount() {
            return this.count;
        }

    }

    /**
     * Construct a new count report.
     * 
     * @param processor     controlling command processor
     * @param file          output file, or NULL to write the STDOUT
     * 
     * @throws IOException
     */
    public CountsTableReporter(BaseTableReporter.IParms processor, File file) throws IOException {
        if (file == null)
            this.writer = new PrintWriter(System.out);
        else
            this.writer = new PrintWriter(file);
        this.counts = new HashMap<>();
        this.controller = processor;
        this.totalCount = 0;
        this.emptyCount = 0;
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        try {
            // Find the ID column index.
            String idColSpec = this.controller.getIdColIdx();
            this.idColIdx = findColumn(colHeaders, idColSpec);
            // Write the output headers. There are two columns-- ID and count.
            this.writer.println(colHeaders.get(this.idColIdx) + "\tCount");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void writeRow(List<Object> fields) {
        // We get the desired ID column value, then count it in the count map.
        // NUlls and blanks are counted in the empty counter.
        Object field = fields.get(this.idColIdx);
        if (field == null)
            this.emptyCount++;
        else {
            String idValue = field.toString();
            if (StringUtils.isBlank(idValue))
                this.emptyCount++;
            else
                this.counts.computeIfAbsent(idValue, k -> new LongCount()).count();
        }
        this.totalCount++;
    }

    @Override
    public void finish() {
        // Now we write the data lines. We start with the empty count, sort the real counts, 
        // write them, and then write the total count.
        writer.println("(none)\t" + this.emptyCount);
        log.info("{} totals found in {} records.", this.counts.size(), this.totalCount);
        // Sort the counts by value and write them.
        long start = System.currentTimeMillis();
        this.counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.comparingLong(LongCount::getCount).reversed()))
                .forEachOrdered(e -> writer.println(e.getKey() + "\t" + e.getValue().getCount()));
        // Write the total count.
        writer.println("TOTAL!\t" + this.totalCount);
        log.info("Finished writing counts in {} ms.", System.currentTimeMillis() - start);
    }

    @Override
    public void close() throws Exception {
        // Close the output writer.
        this.writer.close();
    }

}
