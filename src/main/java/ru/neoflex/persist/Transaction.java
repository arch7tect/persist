package ru.neoflex.persist;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Transaction implements Closeable {
    private final LockManager lockManager;
    private final Set<Long> dirty = new HashSet<>();

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
        Map.Entry<Long, ByteBuffer> entry = lockManager.allocateNew(this);
        return entry;
    }

    public void commit() {
        lockManager.commit(this);
    }

    public synchronized boolean isDirty(long i) {
        return dirty.contains(i);
    }

    public synchronized void setDirty(long i, boolean value) {
        if (value) dirty.add(i);
        else dirty.remove(i);
    }

    public void setDirty(long i) {
        setDirty(i, true);
    }

    public void rollback() {
        lockManager.rollback(this);
    }

    @Override
    public void close() throws IOException {
        commit();
    }
}
