package ru.exportgui.config;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Config {
    private String exportSQLFile = "af5_export.sql";
    private String resultsDir = "results";
    private long version = 1;
    private List<Connection> connections = new ArrayList<>();

    Config() {
    }

    public List<Connection> getConnections() {
        return connections;
    }

    public void setExportSQLFile(String exportSQLFile) {
        this.exportSQLFile = exportSQLFile;
    }

    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    public String getExportSQLFile() {
        return exportSQLFile;
    }

    public String getResultsDir() {
        return resultsDir;
    }

    @JsonIgnore
    public String[] getConnectionNames() {
        String[] arr = new String[0];
        arr = connections.stream().map(Connection::getName).collect(Collectors.toList()).toArray(arr);
        return arr;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }
}
