package ru.neoflex.persist;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public interface LockManager extends Closeable {
    CompletableFuture<ByteBuffer> getPageForRead(Transaction tx, long i);
    CompletableFuture<ByteBuffer> getPageForWrite(Transaction tx, long i);
    void commit(Transaction tx);
    void rollback(Transaction tx);

    default Transaction startTransaction() {
        return new Transaction(this);
    }
}
