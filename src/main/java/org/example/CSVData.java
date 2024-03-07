package org.example;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import com.opencsv.CSVReader;

public class CSVData {
    public static void main(String[] args) {

        String csvPath = csvFilePath();

        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/CSV", "postgres", "1234");
             FileReader fileReader = new FileReader(csvPath);
             CSVReader csvReader = new CSVReader(fileReader)) {

            List<String[]> records = csvReader.readAll();
            if (!records.isEmpty()) {
                String[] columns = records.get(0);
                createTable(connection, "CSVTable_new", columns);

                String insertQuery = prepareInsertQuery(columns);
                try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                    records.stream()
                            .skip(1) // I will Skip first row which is the header row
                            .forEach(values -> {
                                try {
                                    for (int j = 0; j < columns.length; j++) {
                                        preparedStatement.setString(j + 1, values[j]);
                                    }
                                    System.out.println("Inserting data in thread: " + Thread.currentThread().getName() + " : " + new Date());
                                    preparedStatement.executeUpdate();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            });
                }
                System.out.println("Data inserted successfully.");
            } else {
                System.out.println("CSV file is empty.");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static String csvFilePath() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter CSV filepath : ");
        return scanner.nextLine();
    }

    public static void createTable(Connection connection, String tableName, String[] columns) throws SQLException {
        String createTableQuery = buildCreateTableQuery(tableName, columns);
        connection.createStatement().executeUpdate(createTableQuery);
    }

    public static String buildCreateTableQuery(String tableName, String[] columns) {
        String col = Arrays.stream(columns)
                .map(column -> column + " VARCHAR(255)")
                .collect(Collectors.joining(", "));
        return "CREATE TABLE IF NOT EXISTS " + tableName + " (" + col + ")";
    }

    public static String prepareInsertQuery(String[] columns) {
        String insertQuery = "INSERT INTO CSVTable_new VALUES (" +
                String.join(", ", java.util.Collections.nCopies(columns.length, "?")) +
                ")";
        return insertQuery;
    }
}

