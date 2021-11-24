package ru.neoflex.persist;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;

public class BPTree {
    int pageSize;
    Supplier<Key> keyFactory;
    Supplier<Value> valueFactory;
    boolean multivalued = false;
    Node root;

    private static int compareByteArrays(byte[] value, byte[] other) {
        int dl = value.length - other.length;
        if (dl != 0) return dl < 0 ? -1 : 1;
        for (int i = 0; i < other.length; ++i) {
            if (value[i] < other[i]) return -1;
            if (value[i] > other[i]) return 1;
        }
        return 0;
    }

    public static class Builder {
        int pageSize;
        Supplier<Key> keyFactory;
        Supplier<Value> valueFactory;
        boolean multivalued = false;

        public static Builder create() {
            return new Builder();
        }

        public Builder page(int size) {
            assert size >= 0;
            pageSize = size;
            return this;
        }

        public Builder keySupplier(Supplier<Key> factory) {
            this.keyFactory = factory;
            return this;
        }

        public Builder valueSupplier(Supplier<Value> factory) {
            this.valueFactory = factory;
            return this;
        }

        public Builder miltivalued() {
            this.multivalued = true;
            return this;
        }

        private BPTree build() {
            assert keyFactory != null;
            assert valueFactory != null;
            assert pageSize > 0;
            BPTree tree = new BPTree();
            tree.pageSize = pageSize;
            tree.keyFactory = keyFactory;
            tree.valueFactory = valueFactory;
            tree.multivalued = multivalued;
            return tree;
        }

        public BPTree create(Transaction tx) {
            BPTree tree = build();
            LeafNode root = tree.createLeafNode();
            tree.root = root;
            Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
            root.index = entry.getKey();
            root.page = entry.getValue();
            root.next = root.index;
            tree.root.write();
            tx.setDirty(root.index);
            return tree;
        }

        public BPTree load(long index, ByteBuffer page) {
            BPTree tree = build();
            tree.root = tree.createNode(index, page);
            return tree;
        }
    }

    public interface Value {
        int size();
        Object get();
        void read(ByteBuffer buf);
        void write(ByteBuffer buf);
    }

    public interface Key extends Value, Comparable<Key> {}

    public static class LongKey implements Key {
        private long value;

        public static Supplier<LongKey> factory() {
            return () -> new LongKey(0);
        }

        public LongKey(long value) {
            this.value = value;
        }

        @Override
        public int compareTo(Key o) {
            return Long.compare(value, (Long) o.get());
        }

        @Override
        public int size() {
            return 8;
        }

        @Override
        public Object get() {
            return value;
        }

        @Override
        public void read(ByteBuffer buf) {
            value = buf.getLong();
        }

        @Override
        public void write(ByteBuffer buf) {
            buf.putLong(value);
        }
    }

    public static class VarBytesKey implements Key {
        private byte[] value;

        public static Supplier<VarBytesKey> factory() {
            return VarBytesKey::new;
        }

        public VarBytesKey(byte[] value) {
            this.value = value;
        }

        public VarBytesKey() {
            this(new byte[0]);
        }

        @Override
        public int compareTo(Key o) {
            byte[] other = (byte[])o.get();
            return compareByteArrays(value, other);
        }

        @Override
        public int size() {
            return 4 + value.length;
        }

        @Override
        public Object get() {
            return value;
        }

        @Override
        public void read(ByteBuffer buf) {
            int size = buf.getInt();
            value = new byte[size];
            if (size > 0) {
                buf.get(value);
            }
        }

        @Override
        public void write(ByteBuffer buf) {
            buf.putInt(value.length);
            if (value.length > 0) {
                buf.put(value);
            }
        }
    }

    public static class FixedBytesKey implements Key {
        private final byte[] value;

        public static Supplier<FixedBytesKey> factory(int size) {
            return () -> new FixedBytesKey(new byte[size]);
        }

        public FixedBytesKey(byte[] value) {
            this.value = value;
        }

        @Override
        public int compareTo(Key o) {
            byte[] other = (byte[])o.get();
            return compareByteArrays(value, other);
        }

        @Override
        public int size() {
            return value.length;
        }

        @Override
        public Object get() {
            return value;
        }

        @Override
        public void read(ByteBuffer buf) {
            buf.get(value);
        }

        @Override
        public void write(ByteBuffer buf) {
            buf.put(value);
        }
    }

    public static class EmptyValue implements Value {
        public static final Object EMPTY = new Object();

        public static Supplier<EmptyValue> factory() {
            return EmptyValue::new;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Object get() {
            return EMPTY;
        }

        @Override
        public void read(ByteBuffer buf) {
        }

        @Override
        public void write(ByteBuffer buf) {
        }
    }

    abstract class Node {
        public static final int INDEX_PAGE = 1;
        public static final int LEAF_PAGE = 2;
        public static final int LEAF_INCOMPLETE_PAGE = 4|2;
        int nodeType;
        long index;
        IndexNode parent;
        ByteBuffer page;
        abstract int size();
        abstract void read();
        abstract void write();

        public abstract void put(Transaction tx, Key key, Value value);

        protected void allocateParent(Transaction tx) {
            if (parent == null) {
                Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
                parent = createIndexNode();
                // swap pages to keep root index constant
                parent.index = index;
                parent.page = page;
                parent.tail = entry.getKey();
                root = parent;
                index = entry.getKey();
                page = entry.getValue();
            }
        }

        public abstract void delete(Transaction tx, Key key);
    }

    public IndexNode createIndexNode() {
        return new IndexNode();
    }

    public LeafNode createLeafNode() {
        return new LeafNode();
    }

    Node createNode(long index, ByteBuffer page) {
        int nodeType = page.getInt();
        Node node;
        if (nodeType == Node.INDEX_PAGE) {
            node = createIndexNode();
        }
        else if (nodeType == Node.LEAF_PAGE || nodeType == Node.LEAF_INCOMPLETE_PAGE) {
            node = createLeafNode();
        }
        else {
            throw new RuntimeException(String.format("Page <%d> is not BPTree index", index));
        }
        node.index = index;
        node.page = page;
        node.read();
        return node;
    }

    class IndexNode extends Node {
        List<Map.Entry<Key, Long>> entries = new ArrayList<>();
        long tail;

        @Override
        int size() {
            int total = 4;
            total += 4;
            for (Map.Entry<Key, Long> entry: entries) {
                total += entry.getKey().size();
                total += 8;
            }
            total += 8;
            return total;
        }

        @Override
        void read() {
            page.rewind();
            nodeType = page.getInt();
            for (int count = page.getInt(); count > 0; --count) {
                Key key = keyFactory.get();
                key.read(page);
                long value = page.getLong();
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
            tail = page.getLong();
        }

        @Override
        void write() {
            page.rewind();
            page.putInt(nodeType);
            page.putInt(entries.size());
            for (Map.Entry<Key, Long> entry: entries) {
                entry.getKey().write(page);
                page.putLong(entry.getValue());
            }
            page.putLong(tail);
            while (page.hasRemaining()) page.put((byte)0);
        }

        @Override
        public void put(Transaction tx, Key key, Value value) {
            int i = Collections.binarySearch(entries, new AbstractMap.SimpleEntry<>(key, null), Map.Entry.comparingByKey());
            long child = i > 0 ?
                    (i + 1 >= entries.size() ? tail : entries.get(i + 1).getValue()) :
                    (-i - 1 >= entries.size() ? tail : entries.get(-i - 1).getValue());
//            long child = tail;
//            for (Map.Entry<Key, Long> entry: entries) {
//                if (entry.getKey().compareTo(key) > 0) {
//                    child = entry.getValue();
//                    break;
//                }
//            }
            ByteBuffer buffer = tx.getPageForWrite(child);
            Node node = createNode(child, buffer);
            node.parent = this;
            node.put(tx, key, value);
        }

        @Override
        public void delete(Transaction tx, Key key) {

        }

        public void up(Transaction tx, Key key, long index) {
            boolean found = false;
            for (int i = 0; i < entries.size(); ++i) {
                Map.Entry<Key, Long> entry = entries.get(i);
                if (entry.getKey().compareTo(key) > 0) {
                    long oldIndex = entry.getValue();
                    Map.Entry<Key, Long> changedEntry = new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), index);
                    entries.set(i, changedEntry);
                    entries.add(i, new AbstractMap.SimpleImmutableEntry<>(key, oldIndex));
                    found = true;
                    break;
                }
            }
            if (!found) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, tail));
                tail = index;
            }
            if (size() > pageSize) {
                split(tx);
            }
            write();
            tx.setDirty(index);
        }

        private void split(Transaction tx) {
            allocateParent(tx);
            Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
            IndexNode nextNode = createIndexNode();
            nextNode.index = entry.getKey();
            nextNode.page = entry.getValue();
            nextNode.parent = parent;
            nextNode.tail = tail;
            int mid = entries.size() / 2;
            Map.Entry<Key, Long> midEntry = entries.get(mid);
            tail = midEntry.getValue();
            nextNode.entries.addAll(entries.subList(mid + 1, entries.size()));
            entries.subList(mid, entries.size()).clear();
            nextNode.write();
            tx.setDirty(nextNode.index);
            parent.up(tx, midEntry.getKey(), nextNode.index);
        }
    }

    class LeafNode extends Node {
        List<Map.Entry<Key, Value>> entries = new ArrayList<>();
        long prev;
        long next;

        @Override
        int size() {
            int total = 4;
            total += 4;
            for (Map.Entry<Key, Value> entry: entries) {
                total += entry.getKey().size();
                total += entry.getValue().size();
            }
            total += 2*8;
            return total;
        }

        @Override
        void read() {
            page.rewind();
            nodeType = page.getInt();
            for (int count = page.getInt(); count > 0; --count) {
                Key key = keyFactory.get();
                key.read(page);
                Value value = valueFactory.get();
                value.read(page);
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
            prev = page.getLong();
            next = page.getLong();
        }

        @Override
        void write() {
            page.rewind();
            page.putInt(nodeType);
            page.putInt(entries.size());
            for (Map.Entry<Key, Value> entry: entries) {
                entry.getKey().write(page);
                entry.getValue().write(page);
            }
            page.putLong(prev);
            page.putLong(next);
            while (page.hasRemaining()) page.put((byte)0);
        }

        @Override
        public void put(Transaction tx, Key key, Value value) {
            boolean found = false;
            for (int i = 0; i < entries.size(); ++i) {
                Map.Entry<Key, Value> entry = entries.get(i);
                if (entry.getKey().compareTo(key) > 0) {
                    found = true;
                    if (i > 0 && !multivalued && entries.get(i - 1).getKey().compareTo(key) == 0) {
                        entries.set(i - 1, new AbstractMap.SimpleImmutableEntry<>(key, value));
                    }
                    else {
                        entries.add(i, new AbstractMap.SimpleImmutableEntry<>(key, value));
                    }
                }
            }
            if (!found) {
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
            if (size() > pageSize) {
                split(tx);
            }
            write();
            tx.setDirty(index);
        }

        @Override
        public void delete(Transaction tx, Key key) {

        }

        private void split(Transaction tx) {
            allocateParent(tx);
            LeafNode nextNode = allocateNextLeafNode(tx);
            int mid = entries.size() / 2;
            Map.Entry<Key, Value> midEntry = entries.get(mid);
            nextNode.entries.addAll(entries.subList(mid, entries.size()));
            entries.subList(mid, entries.size()).clear();
            nextNode.write();
            tx.setDirty(nextNode.index);
            parent.up(tx, midEntry.getKey(), nextNode.index);
        }

        private LeafNode allocateNextLeafNode(Transaction tx) {
            Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
            LeafNode nextNode = createLeafNode();
            nextNode.index = entry.getKey();
            nextNode.page = entry.getValue();
            nextNode.parent = parent;
            nextNode.next = next;
            nextNode.prev = index;
            next = nextNode.index;
            return nextNode;
        }

    }

    public void put(Transaction tx, Key key, Value value) {
        root.put(tx, key, value);
    }

    public void delete(Transaction tx, Key key) {
        root.delete(tx, key);
    }
}
