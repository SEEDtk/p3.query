package org.theseed.reports;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import org.junit.jupiter.api.Test;


public class TestReporter {

    private static final List<String> HEADERS = List.of("patric_id", "feature.na_length", "genome.aa_length", "solvent");

    @Test
    public void testFindColumn() throws IOException {
        int colNum = BaseTableReporter.findColumn(HEADERS, "1");
        assertThat("Column 1", colNum, is(0));
        colNum = BaseTableReporter.findColumn(HEADERS, "2");
        assertThat("Column 2", colNum, is(1));
        colNum = BaseTableReporter.findColumn(HEADERS, "3");
        assertThat("Column 3", colNum, is(2));
        colNum = BaseTableReporter.findColumn(HEADERS, "4");
        assertThat("Column 4", colNum, is(3));
        colNum = BaseTableReporter.findColumn(HEADERS, "patric_id");
        assertThat("Column patric_id", colNum, is(0));
        colNum = BaseTableReporter.findColumn(HEADERS, "na_length");
        assertThat("Column feature.na_length", colNum, is(1));
        colNum = BaseTableReporter.findColumn(HEADERS, "0");
        assertThat("Column 0", colNum, is(3));
        colNum = BaseTableReporter.findColumn(HEADERS, "-1");
        assertThat("Column -1", colNum, is(2));
    }

}
