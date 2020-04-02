package ru.neoflex.persist;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface IPageFile {
    int getPageSize();
    CompletableFuture<ByteBuffer> readPage(long i);
    public CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page);
}
