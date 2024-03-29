package ru.neoflex.persist;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;

public interface LockManager extends Closeable {
    ByteBuffer getPageForRead(Transaction tx, long i);
    ByteBuffer getPageForWrite(Transaction tx, long i);
    Map.Entry<Long, ByteBuffer> allocateNew(Transaction tx);
    void commit(Transaction tx);
    void rollback(Transaction tx);

    default Transaction startTransaction() {
        return new Transaction(this);
    }
}
