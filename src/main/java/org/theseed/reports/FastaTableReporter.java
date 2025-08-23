package org.theseed.reports;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.theseed.sequence.FastaOutputStream;
import org.theseed.sequence.Sequence;
/**
 * This class converts a table of information into a FASTA file. The first column becomes the
 * ID, the last column becomes the sequence, and there is no comment. This behavior can be modified
 * by the command processor if necessary.
 */

public class FastaTableReporter extends BaseTableReporter {

    // FIELDS
    /** index of ID column */
    private int idColIdx;
    /** index of the sequence column */
    private int seqColIdx;
    /** indices of the comment columns */
    private int[] commentColIdxs;
    /** output FASTA writer */
    private final FastaOutputStream writer;
    /** controlling command processor */
    private final IParms controller;

    public FastaTableReporter(IParms processor, File file) throws IOException {
        // Save the controlling command processor.
        this.controller = processor;
        // Open the output FASTA writer.
        if (file == null)
            this.writer = new FastaOutputStream(System.out);
        else
            this.writer = new FastaOutputStream(file);
    }

    @Override
    public void setHeaders(List<String> colHeaders) {
        try {
            // Here we use the headers to compute the columns that create the output. Note that
            // we don't need to know the actual header names during the report, just their
            // positions.
            String idColSpec = this.controller.getIdColIdx();
            this.idColIdx = findColumn(colHeaders, idColSpec);
            String seqColSpec = this.controller.getSeqColIdx();
            this.seqColIdx = findColumn(colHeaders, seqColSpec);
            // There can be multiple comment columns, so we have to parse for commas.
            String[] commentSpecs = StringUtils.split(this.controller.getCommentColIdxs(), ',');
            this.commentColIdxs = new int[commentSpecs.length];
            for (int i = 0; i < commentSpecs.length; i++)
                this.commentColIdxs[i] = findColumn(colHeaders, commentSpecs[i]);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void writeRow(List<Object> fields) {
        // Get the ID and sequence. If the ID is missing or blank, we skip this record.
        Object id = fields.get(this.idColIdx);
        if (id != null && StringUtils.isNotBlank(id.toString())) {
            String seqId = id.toString();
            // If the sequence is null, we have no sequence.
            Object seqObject = fields.get(this.seqColIdx);
            String sequence;
            if (seqObject == null)
                sequence = "";
            else
                sequence = seqObject.toString();
            // Build the comment.
            String comment = Arrays.stream(commentColIdxs).mapToObj(i -> String.valueOf(fields.get(i))).collect(Collectors.joining("\t"));
            // Form a sequence from the parts.
            Sequence seq = new Sequence(seqId, comment, sequence);
            // Now write the FASTA record.
            try {
                this.writer.write(seq);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void finish() {
        try {
            this.writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.writer.close();
    }

}
