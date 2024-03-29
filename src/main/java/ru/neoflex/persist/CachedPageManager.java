package ru.neoflex.persist;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CachedPageManager implements PageManager {
    private final PageManager pageManager;
    private final LoadingCache<Long, CompletableFuture<ByteBuffer>> cache;

    public CachedPageManager(PageManager pageManager, int maxCacheSize) {
        this.pageManager = pageManager;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .build((i) -> CachedPageManager.this.pageManager.readPage(i));
    }

    @Override
    public int getPageSize() {
        return pageManager.getPageSize();
    }

    @Override
    public Map.Entry<Long, CompletableFuture<ByteBuffer>> allocateNew() {
        Map.Entry<Long, CompletableFuture<ByteBuffer>> result = pageManager.allocateNew();
        cache.put(result.getKey(), result.getValue());
        return result;
    }

    @Override
    public CompletableFuture<ByteBuffer> readPage(long i) {
        return cache.get(i);
    }

    @Override
    public CompletableFuture<ByteBuffer> writePage(long i, ByteBuffer page) {
        CompletableFuture<ByteBuffer> future = pageManager.writePage(i, page);
        cache.put(i, future);
        return future;
    }

    @Override
    public void close() throws IOException {

    }
}
