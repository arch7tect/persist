package ru.neoflex.persist;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BTLongMap {
    private final FileSystemManager manager;
    private long root;

    static class Entry implements Comparable<Entry> {
        public long key;
        public long ptr;

        @Override
        public int compareTo(Entry entry) {
            return Long.compare(key, entry.key);
        }
    }

    class Page {
        long index;
        public long size;
        public List<Entry> entries = new ArrayList<>(maxSize());
        long tail;

        int maxSize() {
            return (manager.getPageSize() - 8 - 4 - 8)/(2*8);
        }

        Page(long index) {
            this.index = index;
        }

        void read(ByteBuffer buffer) {
            buffer.rewind();
            size = buffer.getLong();
            int count = buffer.getInt();
            for (int i = 0; i < count; ++i) {
                Entry entry = new Entry();
                entry.key = buffer.getLong();
                entry.ptr = buffer.getLong();
                entries.add(entry);
            }
            tail = buffer.getLong();
        }

        void write(ByteBuffer buffer) {
            buffer.rewind();
            buffer.putLong(size);
            buffer.putInt(entries.size());
            for (Entry entry: entries) {
                buffer.putLong(entry.key);
                buffer.putLong(entry.ptr);
            }
            buffer.putLong(tail);
        }

        Page put(Transaction tx, long key, long ptr) {
            if (entries.size() >= maxSize()) {
                int middle = entries.size() / 2;
                Entry middleEntry = entries.get(middle);
                Map.Entry<Long, ByteBuffer> left = tx.allocateNew();
                Page leftPage = new Page(left.getKey());
                leftPage.entries.addAll(entries.subList(0, middle + 1));
                Map.Entry<Long, ByteBuffer> right = tx.allocateNew();
                Page rightPage = new Page(right.getKey());
                rightPage.entries.addAll(entries.subList(middle + 1, entries.size()));
                rightPage.tail = tail;
                entries.clear();
                middleEntry.ptr = -left.getKey();
                entries.add(middleEntry);
                tail = right.getKey();
                if (key <= middleEntry.key) {
                    leftPage.put(tx, key, ptr);
                }
                else {
                    rightPage.put(tx, key, ptr);
                }
                write(tx.getPageForWrite(index));
                tx.setDirty(index);
                leftPage.write(left.getValue());
                tx.setDirty(left.getKey());
                rightPage.write(right.getValue());
                tx.setDirty(right.getKey());
            }
            else {
                for (Entry entry: entries) {
                    if (key <= entry.key) {
                        return this;
                    }
                }
            }
            return this;
        }
    }

    public BTLongMap(FileSystemManager manager, long root) {
        this.manager = manager;
        this.root = root;
    }

    public void put(long key, long ptr) {
        Page newRoot = manager.inTransaction(transaction -> {
            ByteBuffer buffer = transaction.getPageForWrite(root);
            Page page = new Page(root);
            page.read(buffer);
            return page.put(transaction, key, ptr);
        });
    }
}
