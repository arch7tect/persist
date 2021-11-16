package ru.neoflex.persist;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Transaction implements Closeable {
    private final LockManager lockManager;

    public Transaction(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    public ByteBuffer getPageForRead(long i) {
        return lockManager.getPageForRead(this, i);
    }

    public ByteBuffer getPageForWrite(long i) {
        return lockManager.getPageForWrite(this, i);
    }

    public Map.Entry<Long, ByteBuffer> allocateNew() {
        return lockManager.allocateNew(this);
    }

    public void commit() {
        lockManager.commit(this);
    }

    public void rollback() {
        lockManager.rollback(this);
    }

    @Override
    public void close() throws IOException {
        commit();
    }
}
