package ru.exportgui;

import ru.exportgui.config.ConfigLoader;

import java.io.File;
import java.net.URISyntaxException;

public class Main {
    static String ABSOLUTE_PATH;
    public static void main(String [] args) throws URISyntaxException {
        ABSOLUTE_PATH = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParentFile().getPath();
        new MainForm(new ConfigLoader(ABSOLUTE_PATH + "/config.json"));
    }
}
