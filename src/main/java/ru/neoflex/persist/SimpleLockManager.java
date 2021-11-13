package ru.neoflex.persist;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleLockManager implements LockManager {

    static class LockEntry {
        public Set<Transaction> readers = new HashSet<>();
        public Set<Transaction> writers = new HashSet<>();
        public CompletableFuture<ByteBuffer> page;
        public ReadWriteLock lock = new ReadWriteLock();
        public LockEntry(CompletableFuture<ByteBuffer> page) {
            this.page = page;
        }
    }

    private final Map<Long, LockEntry> lockPages = new HashMap<>();
    private final PageManager pageManager;
    private final Map<Transaction, Set<Long>> readPages = new HashMap<>();
    private final Map<Transaction, Set<Long>> writePages = new HashMap<>();

    public SimpleLockManager(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    @Override
    public CompletableFuture<ByteBuffer> getPageForRead(Transaction tx, long i) {
        LockEntry lockEntry = getLockEntry(i);
        if (lockEntry.readers.contains(tx) || lockEntry.writers.contains(tx)) {
            return lockEntry.page;
        }
        lockEntry.lock.lockRead();
        beginRead(i, tx, lockEntry);
        return lockEntry.page;
    }

    private synchronized LockEntry getLockEntry(long i) {
        return lockPages.computeIfAbsent(i, index -> new LockEntry(pageManager.readPage(i)));
    }

    private synchronized void beginRead(long i, Transaction tx, LockEntry lockEntry) {
        readPages.computeIfAbsent(tx, transaction -> new HashSet<>()).add(i);
        lockEntry.readers.add(tx);
    }

    private synchronized void beginWrite(long i, Transaction tx, LockEntry lockEntry) {
        writePages.computeIfAbsent(tx, transaction -> new HashSet<>()).add(i);
        lockEntry.writers.add(tx);
    }

    @Override
    public CompletableFuture<ByteBuffer> getPageForWrite(Transaction tx, long i) {
        LockEntry lockEntry = getLockEntry(i);
        lockEntry.lock.lockWrite();
        beginWrite(i, tx, lockEntry);
        return lockEntry.page;
    }

    @Override
    public void commit(Transaction tx) {
        endTransaction(tx, true);
    }

    @Override
    public void rollback(Transaction tx) {
        endTransaction(tx, false);
    }

    private synchronized void endTransaction(Transaction tx, boolean commit) {
        Set<Long> allPages = new HashSet<>();
        for (Long i: readPages.computeIfAbsent(tx, transaction -> new HashSet<>())) {
            allPages.add(i);
            LockEntry lockEntry = lockPages.get(i);
            if (lockEntry != null) {
                lockEntry.lock.unlockRead();
            }
        }
        readPages.remove(tx);
        for (Long i: writePages.computeIfAbsent(tx, transaction -> new HashSet<>())) {
            allPages.add(i);
            LockEntry lockEntry = lockPages.get(i);
            if (lockEntry != null) {
                lockEntry.lock.unlockWrite();
                if (commit) {
                    pageManager.writePage(i, lockEntry.page.join());
                }
            }
        }
        writePages.remove(tx);
        for (long i: allPages) {
            LockEntry lockEntry = lockPages.get(i);
            if (lockEntry != null && lockEntry.readers.isEmpty() && lockEntry.writers.isEmpty()) {
                lockPages.remove(i);
            }

        }
    }

    @Override
    public void close() throws IOException {

    }
}
