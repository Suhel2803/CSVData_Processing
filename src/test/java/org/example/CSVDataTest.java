package org.example;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class CSVDataTest {

    @Test
    public void csvFilePathTest() {
        String expectedPath = "/home/suhelksi191/Downloads/data.csv";
        System.setIn(new java.io.ByteArrayInputStream(expectedPath.getBytes()));

        String actualPath = CSVData.csvFilePath();

        assertEquals(expectedPath, actualPath);
    }

    @Test
    public void buildCreateTableQueryTest() {
        String expectedQuery = "CREATE TABLE IF NOT EXISTS TestTable (col1 VARCHAR(255), col2 VARCHAR(255), col3 VARCHAR(255))";
        String[] columns = {"col1", "col2", "col3"};

        String actualQuery = CSVData.buildCreateTableQuery("TestTable", columns);

        assertEquals(expectedQuery, actualQuery);
    }

    @Test
    public void prepareInsertQueryTest() {
        String expectedQuery = "INSERT INTO CSVTable_new VALUES (?, ?, ?, ?)";
        String[] columns = {"col1", "col2", "col3", "col4"};

        String actualQuery = CSVData.prepareInsertQuery(columns);

        assertEquals(expectedQuery, actualQuery);
    }
}

