package ru.exportgui.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfigLoader {
    private String path;

    public ConfigLoader(String path) {
        this.path = path;
    }

    public Config load() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Path configPath = Paths.get(path);
            Config config;
            if (Files.isRegularFile(configPath)) {
                config = mapper.readValue(configPath.toFile(), Config.class);
            } else {
                config = new Config();
            }
            return config;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void save(Config config) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            Path configPath = Paths.get(path);
            mapper.writeValue(configPath.toFile(), config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
