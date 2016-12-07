package com.canva.queue.common.file;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * File utils class.
 */
public class FileUtils {

    /**
     * Creates a file to disk given {@code file}.
     *
     * @param file File to create
     * @return File instance created
     */
    public static final File createFile(File file) {
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IllegalStateException("Could not create file '" + file.getPath() + "'");
            }
        }

        return file;
    }

    /**
     * Creates directory to disk given {@code path}.
     *
     * @param path Path related to directory to create
     */
    public static final void createDirectory(String path) {
        File file = new File(path);

        if(!file.exists() && !file.mkdirs()) {
            throw new IllegalStateException("Could not create directory '" + path + "'");
        }
    }

}
