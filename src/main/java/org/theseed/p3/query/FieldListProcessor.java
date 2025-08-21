package org.theseed.p3.query;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.theseed.basic.ParseFailureException;
import org.theseed.p3api.BvbrcDataMap;
import org.theseed.p3api.CursorConnection;
import org.theseed.p3api.KeyBuffer;
import org.theseed.reports.BaseTableReporter;
import org.theseed.utils.BaseBvbrcProcessor;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

/**
 * This command lists the fields for a table. It is useful for planning queries. It also
 * provides a cheap way to validate data map files.
 * 
 * The positional parameter is the name of the table to query.
 *
 * The table name must be a user-friendly name according to the selected BV-BRC data map.
 * 
 * The command-line options are as follows.
 * 
 * -h   display command-line usage
 * -v   display more frequent log messages
 * -o   output file for report (if not STDOUT)
 * 
 * --map      the name of the JSON file containing the BV-BRC data map (default is to use the default map)
 * --format   output report format (default is TAB)
 * 
 */
public class FieldListProcessor extends BaseBvbrcProcessor implements BaseTableReporter.IParms {

    // FIELDS
    /** logging facility */
    private static final Logger log = LoggerFactory.getLogger(FieldListProcessor.class);

    // COMMAND-LINE OPTIONS

    /** output file for report */
    @Option(name = "--output", aliases = { "-o" }, metaVar = "report.tbl", usage = "output file for report (if not STDOUT)")
    private File outputFile;

    /** report format */
    @Option(name = "--format", metaVar = "format", usage = "output report format")
    private BaseTableReporter.Type outputFormat;

    // METHODS

    @Override
    protected void setBvbrcDefaults() {
        this.outputFile = null;
        this.outputFormat = BaseTableReporter.Type.TAB;
    }

    @Override
    protected void validateBvbrcParms() throws ParseFailureException, IOException {
        // Verify that the output file is valid.
        if (this.outputFile == null) {
            if (! this.outputFormat.supportsStdOut())
                throw new ParseFailureException("Output format " + this.outputFormat + " does not support standard output.");
            else
                log.info("Report will be written to the standard output.");
        } else {
            if (! this.outputFile.exists()) {
                // Verify we can create a file with this name.
                try (PrintWriter testWriter = new PrintWriter(this.outputFile)) {
                    // If we can open the file, we can write to it.
                    log.info("Report will be written to file {}.", this.outputFile);
                    testWriter.println();
                }
            } else if (! this.outputFile.canWrite())
                throw new IOException("Cannot write to output file " + this.outputFile + ".");
            else
                log.info("Report will be written to file {}.", this.outputFile);
        }
    }

    @Override
    protected void runBvbrcCommand(CursorConnection p3, String table) throws Exception {
        // Firest, we get the table field list from the cursor connection.
        JsonArray fields = p3.getFieldList(table);
        // Get the table descriptor and build a reverse map. We use this to get the user-friendly
        // names for the internal names.
        BvbrcDataMap.Table tableInfo = this.getTableMap();
        Map<String, String> reverseMap = tableInfo.getReverseMap();
        // Finally, get the list of derived fields.
        Set<String> derivedFields = tableInfo.getDerivedFieldNames();
        // We will build the master field list in here so that it comes out in alphabetical order.
        // Each field will be mapped to a list of the report fields.
        TreeMap<String, List<Object>> masterFields = new TreeMap<>();
        for (Object fieldObject : fields) {
            JsonObject field = (JsonObject) fieldObject;
            String internalName = KeyBuffer.getString(field, "name");
            // Check for a user-friendly name.
            String realName = reverseMap.get(internalName);
            // If there is none, then we have to make sure this field is not hidden.
            boolean skip = false;
            if (realName == null) {
                if (derivedFields.contains(internalName))
                    skip = true;
                else {
                    // Here we have an internal field name. Is it hidden by a user-friendly name?
                    // If it is, we need to skip it.
                    String testName = tableInfo.getInternalFieldName(internalName);
                    if (! testName.equals(internalName))
                        skip = true;
                    else {
                        // Here we are keeping this field. It is externally available under its internal name.
                        realName = internalName;
                    }
                }
            }
            if (! skip) {
                // Here we are keeping the field, so we add its output line to the master map.
                List<Object> outputLine = new ArrayList<>(4);
                outputLine.add(realName);
                outputLine.add(internalName);
                outputLine.add(KeyBuffer.getString(field, "type"));
                String multiFlag = (KeyBuffer.getFlag(field, "multiValued") ? "Y" : "");
                outputLine.add(multiFlag);
                masterFields.put(realName, outputLine);
            }
        }
        // We've finished all the fields in the record. Now add the derived fields.
        for (String derivedField : derivedFields) {
            List<Object> outputLine = new ArrayList<>(4);
            outputLine.add(derivedField);
            // For the internal name, we use the field description. This requires getting the descriptor itself.
            BvbrcDataMap.IField descriptor = tableInfo.getInternalFieldData(derivedField);
            outputLine.add(descriptor.getDescription());
            // The type is always "derived", and the multi-valued flag is always empty.
            outputLine.add("derived");
            outputLine.add("");
            masterFields.put(derivedField, outputLine);
        }
        log.info("Found {} fields in table {}.", masterFields.size(), table);
        // We have a sorted map of field names to output lines. Unroll it into the output file.
        try (BaseTableReporter reporter = this.outputFormat.createReporter(this, this.outputFile)) {
            reporter.setHeaders(Arrays.asList("Field Name", "Internal Name", "Type", "Multi-Valued"));
            for (List<Object> outputLine : masterFields.values())
                reporter.writeRow(outputLine);
        }
    }

    @Override
    public String getIdColIdx() {
        // FASTA reports are not appropriate for this action, so we punt.
        return "1";
    }

    @Override
    public String getSeqColIdx() {
        // FASTA reports are not appropriate for this action, so we punt.
        return "2";
    }

    @Override
    public String getCommentColIdxs() {
        // FASTA reports are not appropriate for this action, so we punt.
        return "";
    }

    @Override
    public String getTargetTableName() {
        return this.getTableName();
    }

}
