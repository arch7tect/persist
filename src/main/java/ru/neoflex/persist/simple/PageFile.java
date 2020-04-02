package ru.neoflex.persist.simple;

import ru.neoflex.persist.IPageFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.concurrent.CompletableFuture;

import static java.nio.file.StandardOpenOption.*;

public class PageFile implements IPageFile, Closeable {
    public static final int DEFAULT_PAGE_SIZE = 1024 * 512;

    Path path;
    AsynchronousFileChannel channel;

    public PageFile(Path path) throws IOException {
        this.path = path;
        channel = AsynchronousFileChannel.open(path, READ, WRITE, CREATE);
    }

    @Override
    public int getPageSize() {
        return DEFAULT_PAGE_SIZE;
    }

    public ByteBuffer allocatePage() {
        return ByteBuffer.allocate(getPageSize());
    }

    @Override
    public CompletableFuture<ByteBuffer> readPage(long i) {
        ByteBuffer page = allocatePage();
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        channel.read(page, i * getPageSize(), null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                if (result != getPageSize()) {
                    promise.completeExceptionally(new IOException(MessageFormat.format("Incomplete read page {0} of {1)", i, path.toString())));
                }
                else {
                    promise.complete(page);
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                promise.completeExceptionally(exc);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page) {
        if ((page.limit() - page.position()) != getPageSize()) {
            throw new IllegalArgumentException(MessageFormat.format("Invalid page size {0}", page.capacity()));
        }
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        channel.write(page, i * getPageSize(), null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                if (result != getPageSize()) {
                    promise.completeExceptionally(new IOException(MessageFormat.format("Incomplete write page {0} of {1)", i, path.toString())));
                }
                else {
                    promise.complete(page);
                }

            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                promise.completeExceptionally(exc);
            }
        });
        return promise;
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
