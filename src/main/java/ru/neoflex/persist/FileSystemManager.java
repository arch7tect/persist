package ru.neoflex.persist;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;

public class FileSystemManager implements Closeable {
    public static final int DEFAULT_PAGE_SIZE = 1024 * 128;

    private final PageFile pageFile;
    private final CachedPageManager pageManager;
    private final LockManager lockManager;

    public FileSystemManager(Path path, int pageSize, int maxCacheSize) throws IOException {
        pageFile = new PageFile(path, pageSize);
        pageManager = new CachedPageManager(pageFile, maxCacheSize);
        lockManager = new SimpleLockManager(pageManager);
    }

    public FileSystemManager(Path path, int maxCacheSize) throws IOException {
        this(path, DEFAULT_PAGE_SIZE, maxCacheSize);
    }

    public long getPageSize() {
        return pageFile.getPageSize();
    }

    public Transaction startTransaction() {
        return lockManager.startTransaction();
    }

    public void inTransaction(Consumer<Transaction> code) throws IOException {
        try (Transaction tx = startTransaction()) {
            code.accept(tx);
        }
    }

    @Override
    public void close() throws IOException {
        pageManager.close();
        pageFile.close();
    }
}
