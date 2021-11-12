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
        final long SIZE = 500;
        try {
            try (PageFile pageFile = new PageFile(path)) {
                CachedPageManager cache = new CachedPageManager(pageFile, 10);
                List<CompletableFuture<ByteBuffer>> allWrites = new ArrayList<>();
                for (long i = 0; i < SIZE; ++i) {
                    ByteBuffer buf = pageFile.allocatePage();
                    buf.put(MessageFormat.format("Page {0}", i).getBytes());
                    buf.position(0);
                    CompletableFuture<ByteBuffer> future = cache.writePage(i, buf);
                    allWrites.add(future);
                }
                CompletableFuture.allOf(allWrites.toArray(new CompletableFuture[0])).get();
                List<CompletableFuture<ByteBuffer>> allReads = new ArrayList<>();
                for (long i = 0; i < SIZE; ++i) {
                    byte[] magic = MessageFormat.format("Page {0}", i).getBytes();
                    CompletableFuture<ByteBuffer> future = cache.readPage(i).thenApply(byteBuffer -> {
                        Assert.assertEquals(pageFile.getPageSize(), byteBuffer.remaining());
                        byte[] real = new byte[magic.length];
                        byteBuffer.get(real);
                        Assert.assertArrayEquals(magic, real);
                        return byteBuffer;
                    });
                    allReads.add(future);
                }
                CompletableFuture.allOf(allReads.toArray(new CompletableFuture[0])).get();
            }
        }
        finally {
            Files.deleteIfExists(path);
        }
    }
}
