package ru.exportgui;

import ru.exportgui.config.Connection;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

class QueryExecutor {
    private String resultFileName;

    String getResultFileName() {
        return resultFileName;
    }

    private Path getWithAbsolutePath(String str) {
        return Paths.get(Main.ABSOLUTE_PATH).resolve(str);
    }

    QueryExecutor(Connection connection, String inputFileName, String outputDir) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss");
        resultFileName = Paths.get(
                outputDir,
                connection.getName() + "_" + ZonedDateTime.now().format(formatter)
        ).toString() + ".sql";
        try {
            String script = new String(
                    Files.readAllBytes(getWithAbsolutePath(inputFileName)), StandardCharsets.UTF_8
            );
            Properties properties = new Properties();
            properties.setProperty("user", connection.getLogin());
            properties.setProperty("password", connection.getPassword());
            properties.setProperty("readOnly", "true");
            try (
                    java.sql.Connection sqlConnection = DriverManager.getConnection(
                            "jdbc:postgresql://" + connection.getUrl(),
                            properties)
            ) {
                StringBuilder dataQuery = new StringBuilder();
                try (
                        Statement statement = sqlConnection.createStatement();
                        ResultSet resultSet = statement.executeQuery(script)
                ) {
                    while (resultSet.next()) {
                        dataQuery.append(resultSet.getString(1));
                        dataQuery.append("\n");
                    }
                }
                //noinspection ResultOfMethodCallIgnored
                getWithAbsolutePath(resultFileName).toFile().createNewFile();
                try (
                        Statement statement = sqlConnection.createStatement();
                        ResultSet resultSet = statement.executeQuery(dataQuery.toString());
                        OutputStreamWriter outputStream = new OutputStreamWriter(
                                new FileOutputStream(
                                        getWithAbsolutePath(resultFileName).toFile(), false
                                ),
                                Charset.forName("UTF-8").newEncoder()
                        )
                ) {
                    while (resultSet.next()) {
                        outputStream.write(resultSet.getString(1));
                        outputStream.write("\n");
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
