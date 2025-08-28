package org.theseed.p3.query;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.kohsuke.args4j.Option;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.BvbrcDataMap;
import org.theseed.p3api.CursorConnection;
import org.theseed.reports.BaseTableReporter;
import org.theseed.utils.BaseCursorProcessor;

/**
 * This command lists the tables in the database according to a specified table map. It is useful for planning queries.
 * 
 * There is no positional parameter.
 *
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -o   output file for report (if not STDOUT)
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --format   the output report format (default is TAB)
 * 
 */
public class TableListProcessor extends BaseCursorProcessor implements BaseTableReporter.IParms {

    // FIELDS
    /** report writer */
    private BaseTableReporter reportWriter;

    // COMMAND-LINE OPTIONS

    /** output file for report (if not STDOUT) */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "outFile", usage = "output file for report (if not STDOUT)")
    private File outputFile;

    /** output format */
    @Option(name = "--format", usage = "the output format")
    private BaseTableReporter.Type outputFormat;

    @Override
    protected void setCursorDefaults() {
        this.outputFile = null;
        this.outputFormat = BaseTableReporter.Type.TAB;
    }

    @Override
    protected void validateCursorParms(CursorConnection p3) throws ParseFailureException, IOException {
        // Create the report writer.
        this.reportWriter = this.outputFormat.createReporter(this, this.outputFile);
    }

    @Override
    protected void runCursorCommand(CursorConnection p3) throws Exception {
        try {
            // We have two output columns-- user-friendly name and internal name.
            this.reportWriter.setHeaders(Arrays.asList("User-Friendly Name", "Internal Name"));
            // Get the data map.
            BvbrcDataMap dataMap = this.getDataMap();
            for (var entry : dataMap) {
                String tableName = entry.getKey();
                String internalName = entry.getValue().getInternalName();
                this.reportWriter.writeRow(Arrays.asList(tableName, internalName));
            }
        } finally {
            this.reportWriter.close();
        }
    }

    @Override
    public String getIdColIdx() {
        throw new IllegalArgumentException("This report format is not supported for table lists.");
    }

    @Override
    public String getSeqColIdx() {
        throw new IllegalArgumentException("This report format is not supported for table lists.");
    }

    @Override
    public String getCommentColIdxs() {
        throw new IllegalArgumentException("This report format is not supported for table lists.");
    }

    @Override
    public String getTargetTableName() {
        throw new IllegalArgumentException("This report format is not supported for table lists.");
    }

}
