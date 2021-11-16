package ru.neoflex.persist;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PageFileTest {
    @Test
    public void emptyTest() throws IOException, ExecutionException, InterruptedException {
        Path path = Files.createTempFile("test_", "");
        final long SIZE = 1024;
        try {
            try (FileSystemManager fs = new FileSystemManager(path, 64*1024, 512)) {
                for (long i = 0; i < SIZE; ++i) {
                    byte[] content = MessageFormat.format("Page {0}", i).getBytes();
                    try (Transaction tx = fs.startTransaction()) {
                        ByteBuffer buf = tx.allocateNew().getValue();
                        buf.put(content);
                    }
                }
                for (long i = 0; i < SIZE; ++i) {
                    try (Transaction tx = fs.startTransaction()) {
                        byte[] magic = MessageFormat.format("Page {0}", i).getBytes();
                        ByteBuffer byteBuffer = tx.getPageForRead(i);
                        Assert.assertEquals(fs.getPageSize(), byteBuffer.remaining());
                        byte[] real = new byte[magic.length];
                        byteBuffer.get(real);
                        Assert.assertArrayEquals(magic, real);
                    }
                }
            }
        } finally {
            Files.deleteIfExists(path);
        }
    }
}
