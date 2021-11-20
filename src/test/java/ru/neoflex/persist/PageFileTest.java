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
import java.util.Random;
import java.util.concurrent.*;

public class PageFileTest {
    @Test
    public void emptyTest() throws IOException, ExecutionException, InterruptedException {
        Path path = Files.createTempFile("test_", "");
        final long SIZE = 1024;
        try {
            try (FileSystemManager fs = new FileSystemManager(path, 64*1024, 512)) {
                for (long i = 0; i < SIZE; ++i) {
                    byte[] content = MessageFormat.format("Page {0}", i).getBytes();
                    fs.inTransaction(tx -> {
                        ByteBuffer buf = tx.allocateNew().getValue();
                        buf.put(content);
                    });
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

    @Test
    public void mtTest() throws IOException {
        final int PAGE_SIZE = 1024*64;
        final long PAGE_COUNT = 1024;
        final int CACHE_SIZE = 128;
        final int THREAD_COUNT = 64;
        final int TASK_COUNT = 1024/**128*/;
        Path path = Files.createTempFile("test_", "");
        try {
            try (FileSystemManager fs = new FileSystemManager(path, PAGE_SIZE, CACHE_SIZE)) {
                for (long i = 0; i < PAGE_COUNT; ++i) {
                    byte[] content = MessageFormat.format("Page {0}", i).getBytes();
                    fs.inTransaction(tx -> {
                        ByteBuffer buf = tx.allocateNew().getValue();
                        buf.put(content);
                    });
                }
                ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
                Random random = new Random();
                for (int i = 0; i < TASK_COUNT; ++i) {
                    long pNo = Math.abs(random.nextLong())%PAGE_COUNT;
                    byte[] content = MessageFormat.format("Task {0}", i).getBytes();
                    executor.submit(() -> {
                        fs.inTransaction(tx->{
                            tx.getPageForWrite(pNo).put(content);
                        });
                        fs.inTransaction(tx->{
                            ByteBuffer buf = tx.getPageForRead(pNo);
                            byte[] page = new byte[content.length];
                            buf.get(page);
                            Assert.assertArrayEquals(page, content);
                        });
                    });
                }
            }
        }
        finally {
            Files.deleteIfExists(path);
        }
    }
}
