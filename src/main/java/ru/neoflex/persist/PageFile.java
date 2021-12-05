package ru.neoflex.persist;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.*;

public class PageFile implements PageManager {
    private AtomicLong pageCount;
    private int pageSize;
    private AsynchronousFileChannel channel;

    public PageFile(Path path, int pageSize) throws IOException {
        this.pageSize = pageSize;
        channel = AsynchronousFileChannel.open(path, READ, WRITE, CREATE);
        long size = channel.size();
        pageCount = new AtomicLong(size / pageSize);
        if (size % pageSize != 0) pageCount.incrementAndGet();
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public Map.Entry<Long, CompletableFuture<ByteBuffer>> allocateNew() {
        long i = pageCount.getAndIncrement();
        ByteBuffer page = allocatePage();
        CompletableFuture<ByteBuffer> f = writePage(i, page);
        return new AbstractMap.SimpleImmutableEntry<>(i, f);
    }

    public ByteBuffer allocatePage() {
        ByteBuffer buf = ByteBuffer.allocate(getPageSize());
        final int offset = buf.arrayOffset();
        Arrays.fill(buf.array(), offset + buf.position(), offset + buf.limit(), (byte) 0);
        return buf;
    }

    @Override
    public CompletableFuture<ByteBuffer> readPage(long i) {
        assert 0 <= i && i < pageCount.get();
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        ByteBuffer page = allocatePage();
        long position = i * getPageSize();
        channel.read(page, position, position, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, Long attachment) {
                if (result <= 0) {
                    promise.completeExceptionally(new IllegalArgumentException(
                            MessageFormat.format("Illegal file position {0}", position)));
                } else if (page.remaining() > 0) {
                    long newPosition = attachment + result;
                    channel.read(page, newPosition, newPosition, this);
                } else {
                    promise.complete(page.flip());
                }
            }

            @Override
            public void failed(Throwable exc, Long attachment) {
                promise.completeExceptionally(exc);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page) {
        assert 0 <= i && i < pageCount.get();
        page.rewind();
        assert page.remaining() == getPageSize();
        long position = i * getPageSize();
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        channel.write(page, position, position, new CompletionHandler<>() {
            @Override
            public void completed(Integer result, Long attachment) {
                if (page.remaining() > 0) {
                    long newPosition = attachment + result;
                    channel.write(page, newPosition, newPosition, this);
                } else {
                    promise.complete(page.rewind());
                }
            }

            @Override
            public void failed(Throwable exc, Long attachment) {
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
