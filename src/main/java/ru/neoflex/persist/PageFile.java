package ru.neoflex.persist;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static java.nio.file.StandardOpenOption.*;

public class PageFile implements PageManager {
    public static final int DEFAULT_PAGE_SIZE = 1024 * 128;

    private int pageSize;
    private AsynchronousFileChannel channel;

    public PageFile(Path path) throws IOException {
        this(path, DEFAULT_PAGE_SIZE);
    }

    public PageFile(Path path, int pageSize) throws IOException {
        this.pageSize = pageSize;
        channel = AsynchronousFileChannel.open(path, READ, WRITE, CREATE);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    public ByteBuffer allocatePage() {
        ByteBuffer buf = ByteBuffer.allocate(getPageSize());
        final int offset = buf.arrayOffset();
        Arrays.fill(buf.array(), offset + buf.position(), offset + buf.limit(), (byte) 0);
        //buf.position(buf.limit());
        return buf;
    }

    @Override
    public CompletableFuture<ByteBuffer> readPage(long i) {
        ByteBuffer page = allocatePage();
        return read(i * getPageSize(), page, true).thenApply(count -> page.flip());
    }

    public CompletableFuture<Integer> read(long position, ByteBuffer page, boolean readAll) {
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        int remaining = page.remaining();
        if (remaining == 0) {
            promise.complete(0);
        } else {
            channel.read(page, position, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    if (result <= 0) {
                        promise.completeExceptionally(new IllegalArgumentException(
                                MessageFormat.format("Illegal file position {0}", position)));
                    } else if ((result < remaining) && readAll) {
                        read(position + result, page, readAll).thenAccept(
                                rest -> promise.complete(result + rest));
                    } else {
                        promise.complete(result);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    promise.completeExceptionally(exc);
                }
            });
        }
        return promise;
    }

    @Override
    public CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page) {
        return write(i * getPageSize(), page, true).thenApply(count -> page);
    }

    public CompletableFuture<Integer> write(long position, ByteBuffer page, boolean writeFull) {
        long remaining = page.remaining();
        CompletableFuture<Integer> promise = new CompletableFuture<>();
        if (remaining == 0) {
            promise.complete(0);
        } else {
            channel.write(page, position, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    if ((result < remaining) && writeFull) {
                        write(position + result, page, writeFull).handle((rest, throwable) -> {
                            if (rest != null) {
                                promise.complete(result + rest);
                            } else {
                                promise.completeExceptionally(throwable);
                            }
                            return null;
                        });
                    } else {
                        promise.complete(result);
                    }

                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    promise.completeExceptionally(exc);
                }
            });
        }
        return promise;
    }

    @Override
    public void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
