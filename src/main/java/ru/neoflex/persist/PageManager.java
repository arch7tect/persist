package ru.neoflex.persist;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PageManager extends Closeable {
    int getPageSize();
    Map.Entry<Long, CompletableFuture<ByteBuffer>> allocateNew();
    CompletableFuture<ByteBuffer> readPage(long i);
    CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page);
}
