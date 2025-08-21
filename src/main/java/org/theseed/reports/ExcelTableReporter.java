
package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.theseed.excel.CustomWorkbook;

/**
 * This report-writer generates an Excel spreadsheet table in a named file. It does not support writing to
 * STDOUT at the current time.
 * 
 * The table column names are the header names. We interrogate field types to determine how to store each
 * field.
 */
public class ExcelTableReporter extends BaseTableReporter {

    // FIELDS
    /** worksheet controller */
    private CustomWorkbook worksheet;

    public ExcelTableReporter(IParms processor, File file) throws IOException {
        if (file == null)
            throw new IOException("Cannot use STDOUT for an Excel report.");
        this.worksheet = CustomWorkbook.create(file);
        // We will put the data in a sheet with the same name as the target table.
        this.worksheet.addSheet(processor.getTargetTableName(), true);
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        this.worksheet.setHeaders(colHeaders);
    }

    @Override
    public void writeRow(List<Object> fields) {
        // Make room for the new data in the table.
        this.worksheet.addRow();
        // Unspool the data into the current row.
        for (Object field : fields) {
            if (field == null) {
                this.worksheet.storeBlankCell();
            } else if (field instanceof String fieldString) {
                this.worksheet.storeCell(fieldString, CustomWorkbook.Text.NORMAL);
            } else if (field instanceof Number fieldDouble) {
                this.worksheet.storeCell(fieldDouble.doubleValue(), CustomWorkbook.Num.NORMAL);
            } else if (field instanceof Boolean fieldBoolean) {
                this.worksheet.storeCell(fieldBoolean ? "Y" : "", CustomWorkbook.Text.FLAG);
            } else {
                this.worksheet.storeCell(field.toString(), CustomWorkbook.Text.NORMAL);
            }
        }
    }

    @Override
    public void finish() {
    }

    @Override
    public void close() throws Exception {
        this.worksheet.close();
    }

}
