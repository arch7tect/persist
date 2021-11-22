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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.logging.Logger;

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
                        Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
                        ByteBuffer buf = entry.getValue();
                        buf.put(content);
                        tx.setDirty(entry.getKey());
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
    public void mtTest() throws IOException, InterruptedException {
        final int PAGE_SIZE = 16*1024;
        final long PAGE_COUNT = 2*1024;
        final int CACHE_SIZE = 1024;
        final int THREAD_COUNT = 32;
        final int TASK_COUNT = 1024;
        Path path = Files.createTempFile("test_", "");
        try {
            try (FileSystemManager fs = new FileSystemManager(path, PAGE_SIZE, CACHE_SIZE)) {
                for (long i = 0; i < PAGE_COUNT; ++i) {
                    fs.inTransaction(tx -> {
                        tx.allocateNew();
                     });
                }
                ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
                for (int i = 0; i < TASK_COUNT; ++i) {
                   long task = i;
                   long pNo = i%PAGE_COUNT;
                   executor.submit(() -> {
                        fs.inTransaction(tx->{
                            ByteBuffer buf = tx.getPageForWrite(pNo);
                            if (buf.array()[0] != 0) {
                                byte[] content = new byte[buf.remaining()];
                                buf.get(content);
                                Logger.getLogger(this.getClass().getSimpleName()).info(new String(content));
                                buf.rewind();
                            }
                            byte[] content = MessageFormat.format("Task {0}, Page {1}", task, pNo).getBytes();
                            buf.put(content);
                            tx.setDirty(pNo);
                        });
                        fs.inTransaction(tx->{
                            ByteBuffer buf = tx.getPageForRead(pNo);
                            byte[] content = MessageFormat.format("Task {0}, Page {1}", task, pNo).getBytes();
                            byte[] page = new byte[content.length];
                            buf.get(page);
                            Assert.assertArrayEquals(page, content);
                        });
                    });
                }
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {;}
            }
        }
        finally {
            Files.deleteIfExists(path);
        }
    }
}
