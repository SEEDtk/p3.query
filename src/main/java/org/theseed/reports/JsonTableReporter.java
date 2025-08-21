package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.github.cliftonlabs.json_simple.JsonObject;
import com.github.cliftonlabs.json_simple.Jsoner;

/**
 * This report outputs the table as a giant JSON list. It will not be pretty.
 */

public class JsonTableReporter extends BaseTableReporter {

    // FIELDS
    /** output print writer */
    private final PrintWriter writer;
    /** list of column headers */
    private List<String> colHeaders;
    /** TRUE if there is already an object in the output */
    private boolean hasObject;

    public JsonTableReporter(IParms processor, File file) throws IOException {
        if (file == null) {
            this.writer = new PrintWriter(System.out);
        } else {
            this.writer = new PrintWriter(file);
        }
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        // Save the headers, because we need them to form the JSON objects.
        this.colHeaders = colHeaders;
        // Write an open square bracket. We do not print the new-line. The report expects
        // each row to start at the end of the previous row.
        this.writer.print("[");
    }

    @Override
    public void writeRow(List<Object> fields) {
        // Create a JSON object for the row
        JsonObject jsonRow = new JsonObject();
        for (int i = 0; i < this.colHeaders.size(); i++) {
            Object obj = fields.get(i);
            // Convert nulls to an empty string. Everything else is handled by Jsoner.
            if (obj == null) obj = "";
            jsonRow.put(this.colHeaders.get(i), obj);
        }
        // Convert the row to a string.
        String jsonString = Jsoner.serialize(jsonRow);
        // Write the object.
        if (hasObject) {
            this.writer.println(",");
        } else {
            hasObject = true;
            this.writer.println();
        }
        this.writer.print(jsonString);
    }

    @Override
    public void finish() {
        // Write the closing bracket for the JSON array
        this.writer.println();
        this.writer.println("]");
        this.writer.flush();
    }

    @Override
    public void close() throws Exception {
        this.writer.close();
    }

}
