package ru.neoflex.persist;

import org.junit.Test;
import ru.neoflex.persist.simple.PageFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class PageFileTest {
    @Test
    public void emptyTest() throws IOException {
        Path path = Files.createTempFile("test_", "");
        try (PageFile pageFile = new PageFile(path)) {

        }
    }
}
