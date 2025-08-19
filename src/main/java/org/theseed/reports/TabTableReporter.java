package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * This report object produces a table report as a tab-delimited flat file.
 */
public class TabTableReporter extends BaseTableReporter {

    // FIELDS
    /** output print writer */
    private PrintWriter writer;

    public TabTableReporter(IParms processor, File file) throws IOException {
        // Create the print writer.
        this.writer = new PrintWriter(file);
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        this.writer.println(StringUtils.join(colHeaders, "\t"));
    }

    @Override
    public void writeRow(List<String> fields) {
        this.writer.println(StringUtils.join(fields, "\t"));
    }

    @Override
    public void finish() {
        this.writer.flush();
    }

    @Override
    public void close() {
        if (this.writer != null) {
            this.writer.close();
        }
    }

}
