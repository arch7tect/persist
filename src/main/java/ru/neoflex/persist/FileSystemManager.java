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

    public int getPageSize() {
        return pageFile.getPageSize();
    }

    public Transaction startTransaction() {
        return lockManager.startTransaction();
    }

    public void inTransaction(Consumer<Transaction> code) {
        try (Transaction tx = startTransaction()) {
            code.accept(tx);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        lockManager.close();
        pageManager.close();
        pageFile.close();
    }
}
