package ru.neoflex.persist;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
        public long index;
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
            buffer.putLong(size);
            buffer.putInt(entries.size());
            for (Entry entry: entries) {
                buffer.putLong(entry.key);
                buffer.putLong(entry.ptr);
            }
            buffer.putLong(tail);
        }
    }

    public BTLongMap(FileSystemManager manager, long root) {
        this.manager = manager;
        this.root = root;
    }

    public void put(long key, long ptr) {
        manager.inTransaction(transaction -> {

        });
    }
}
