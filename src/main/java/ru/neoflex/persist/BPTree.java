package ru.neoflex.persist;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class BPTree {
    int pageSize;
    int fixedKeySize = -1;
    int fixedValueSize = -1;
    Supplier<Key> keyFactory;
    Supplier<Value> valueFactory;
    boolean multivalued = false;
    Node root;

    public static class Builder {
        int pageSize;
        int fixedKeySize = -1;
        int fixedValueSize = -1;
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

        public Builder fixedKey(int size) {
            assert size >= 0;
            fixedKeySize = size;
            return this;
        }

        public Builder fixedValue(int size) {
            assert size >= 0;
            fixedValueSize = size;
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
            tree.fixedKeySize = fixedKeySize;
            tree.fixedValueSize = fixedValueSize;
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
        void read(ByteBuffer buf, int size);
        void write(ByteBuffer buf, int size);
    }

    public interface Key extends Value, Comparable<Key> {}

    public static class LongKey implements Key {
        public static final Supplier<LongKey> FACTORY = () -> new LongKey(0);
        private long value;

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
        public void read(ByteBuffer buf, int size) {
            assert size == size();
            value = buf.getLong();
        }

        @Override
        public void write(ByteBuffer buf, int size) {
            assert size == size();
            buf.putLong(value);
        }
    }

    public static class EmptyValue implements Value {
        public static final Object EMPTY = new Object();
        public static final Supplier<EmptyValue> FACTORY = EmptyValue::new;

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Object get() {
            return EMPTY;
        }

        @Override
        public void read(ByteBuffer buf, int size) {
            assert size == 0;
        }

        @Override
        public void write(ByteBuffer buf, int size) {
            assert size == 0;
        }
    }

    abstract class Node {
        public static final int INDEX_PAGE = 1;
        public static final int LEAF_PAGE = 2;
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
        if (nodeType != Node.INDEX_PAGE && nodeType != Node.LEAF_PAGE) {
            throw new RuntimeException(String.format("Page <%d> is not BPTree index", index));
        }
        Node node = nodeType == Node.INDEX_PAGE ? createIndexNode() : createLeafNode();
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
                if (fixedKeySize < 0) {
                    total += 4;
                    total += entry.getKey().size();
                }
                else {
                    total += fixedKeySize;
                }
                total += 8;
            }
            total += 8;
            return total;
        }

        @Override
        void read() {
            page.rewind();
            assert page.getInt() == Node.INDEX_PAGE;
            for (int count = page.getInt(); count > 0; --count) {
                Key key = keyFactory.get();
                int keySize = fixedKeySize >= 0 ? fixedKeySize : page.getInt();
                key.read(page, keySize);
                long value = page.getLong();
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
            tail = page.getLong();
        }

        @Override
        void write() {
            page.rewind();
            page.putInt(Node.INDEX_PAGE);
            page.putInt(entries.size());
            for (Map.Entry<Key, Long> entry: entries) {
                if (fixedKeySize < 0) {
                    int keySize = entry.getKey().size();
                    page.putInt(keySize);
                    entry.getKey().write(page, keySize);
                }
                else {
                    entry.getKey().write(page, fixedKeySize);
                }
                page.putLong(entry.getValue());
            }
            page.putLong(tail);
            while (page.hasRemaining()) page.put((byte)0);
        }

        @Override
        public void put(Transaction tx, Key key, Value value) {
            long child = tail;
            for (Map.Entry<Key, Long> entry: entries) {
                if (entry.getKey().compareTo(key) > 0) {
                    child = entry.getValue();
                    break;
                }
            }
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
        long next;

        @Override
        int size() {
            int total = 4;
            total += 4;
            for (Map.Entry<Key, Value> entry: entries) {
                if (fixedKeySize < 0) {
                    total += 4;
                    total += entry.getKey().size();
                }
                else {
                    total += fixedKeySize;
                }
                if (fixedValueSize < 0) {
                    total += 4;
                    total += entry.getValue().size();
                }
                else {
                    total += fixedValueSize;
                }
            }
            total += 8;
            return total;
        }

        @Override
        void read() {
            page.rewind();
            assert page.getInt() == Node.LEAF_PAGE;
            for (int count = page.getInt(); count > 0; --count) {
                Key key = keyFactory.get();
                int keySize = fixedKeySize >= 0 ? fixedKeySize : page.getInt();
                key.read(page, keySize);
                Value value = valueFactory.get();
                int valueSize = fixedValueSize >= 0 ? fixedValueSize : page.getInt();
                value.read(page, valueSize);
                entries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
            }
            next = page.getLong();
        }

        @Override
        void write() {
            page.rewind();
            page.putInt(Node.LEAF_PAGE);
            page.putInt(entries.size());
            for (Map.Entry<Key, Value> entry: entries) {
                if (fixedKeySize < 0) {
                    int keySize = entry.getKey().size();
                    page.putInt(keySize);
                    entry.getKey().write(page, keySize);
                }
                else {
                    entry.getKey().write(page, fixedKeySize);
                }
                if (fixedValueSize < 0) {
                    int valueSize = entry.getValue().size();
                    page.putInt(valueSize);
                    entry.getValue().write(page, valueSize);
                }
                else {
                    entry.getValue().write(page, fixedValueSize);
                }
            }
            page.putLong(next);
            while (page.hasRemaining()) page.put((byte)0);
        }

        protected void allocateParent(Transaction tx) {
            super.allocateParent(tx);
            next = index;
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
            Map.Entry<Long, ByteBuffer> entry = tx.allocateNew();
            LeafNode nextNode = createLeafNode();
            nextNode.index = entry.getKey();
            nextNode.page = entry.getValue();
            nextNode.parent = parent;
            nextNode.next = next;
            next = nextNode.index;
            int mid = entries.size() / 2;
            Map.Entry<Key, Value> midEntry = entries.get(mid);
            nextNode.entries.addAll(entries.subList(mid, entries.size()));
            entries.subList(mid, entries.size()).clear();
            nextNode.write();
            tx.setDirty(nextNode.index);
            parent.up(tx, midEntry.getKey(), nextNode.index);
        }

    }

    public void put(Transaction tx, Key key, Value value) {
        root.put(tx, key, value);
    }

    public void delete(Transaction tx, Key key) {
        root.delete(tx, key);
    }
}
