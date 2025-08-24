package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.TextStringBuilder;

import com.github.cliftonlabs.json_simple.JsonArray;

/**
 * This report object produces a table report as a tab-delimited flat file.
 */
public class TabTableReporter extends BaseTableReporter {

    // FIELDS
    /** output print writer */
    private final PrintWriter writer;
    /** delimiter to use for list results */
    private static final String DELIM = "::";

    public TabTableReporter(IParms processor, File file) throws IOException {
        // Create the print writer.
        if (file == null)
            this.writer = new PrintWriter(System.out);
        else
            this.writer = new PrintWriter(file);
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        this.writer.println(StringUtils.join(colHeaders, "\t"));
    }

    @Override
    public void writeRow(List<Object> fields) {
        // Convert all our field values to strings.
        TextStringBuilder outLine = new TextStringBuilder(fields.size() * 10);
        for (Object field : fields) {
            // Add a delimiter if needed.
            outLine.appendSeparator("\t");
            // We need to process according to the type of the field.
            if (field == null) {
                // This is a null field, so we just append an empty string.
                outLine.append("");
            } else if (field instanceof String str) {
                // This is a string field (the most common), so we append it as-is.
                outLine.append(str);
            } else if (field instanceof JsonArray itemList) {
                // This is a list of items, so we need to join them with double-colons.
                boolean first = true;
                for (Object item : itemList) {
                    if (! first) outLine.append(DELIM);
                    outLine.append(String.valueOf(item));
                    first = false;
                }
            } else {
                // Here we have a singleton field of unknown type that we convert to a string.
                String string = String.valueOf(field);
                outLine.append(string);
            }
        }
        this.writer.println(outLine.toString());
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
