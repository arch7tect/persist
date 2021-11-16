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
        final long SIZE = 256;
        try {
            try (FileSystemManager fs = new FileSystemManager(path, 64000, 64)) {
                List<CompletableFuture<ByteBuffer>> allWrites = new ArrayList<>();
                for (long i = 0; i < SIZE; ++i) {
                    byte[] content = MessageFormat.format("Page {0}", i).getBytes();
                    try (Transaction tx = fs.startTransaction()) {
                        CompletableFuture<ByteBuffer> future = tx.allocateNew().getValue().thenApply(buf -> {
                            buf.rewind().put(content);
                            return buf;
                        });
                        allWrites.add(future);
                    }
                }
                CompletableFuture.allOf(allWrites.toArray(new CompletableFuture[0])).get();
                List<CompletableFuture<ByteBuffer>> allReads = new ArrayList<>();
                for (long i = 0; i < SIZE; ++i) {
                    try (Transaction tx = fs.startTransaction()) {
                        byte[] magic = MessageFormat.format("Page {0}", i).getBytes();
                        CompletableFuture<ByteBuffer> future = tx.getPageForRead(i).thenApply(byteBuffer -> {
                            Assert.assertEquals(fs.getPageSize(), byteBuffer.remaining());
                            byte[] real = new byte[magic.length];
                            byteBuffer.rewind().get(real);
                            Assert.assertArrayEquals(magic, real);
                            return byteBuffer;
                        });
                        allReads.add(future);
                    }
                }
                CompletableFuture.allOf(allReads.toArray(new CompletableFuture[0])).get();
            }
        }
        finally {
            Files.deleteIfExists(path);
        }
    }
}
