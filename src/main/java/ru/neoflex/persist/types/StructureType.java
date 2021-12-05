package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.*;

public class StructureType implements Type {
    boolean allowNulls;
    Map.Entry<String, Type>[] entries;

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new StructureType.Super();

        @Override
        public String getName() {
            return "structure";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            StructureType structureType = (StructureType) value;
            int size = 1 + 4;
            for (Map.Entry<String, Type> entry: structureType.entries) {
                size += StringType.INSTANCE.size(entry.getKey());
                size += Registry.INSTANCE.size(entry.getValue());
            }
            return size;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            StructureType structureType = (StructureType) value;
            buffer.put((byte) (structureType.allowNulls ? 1 : 0));
            buffer.putInt(structureType.entries.length);
            for (Map.Entry<String, Type> entry: structureType.entries) {
                new StringType().write(buffer, entry.getKey());
                Registry.INSTANCE.write(buffer, entry.getValue());
            }
        }

        @Override
        public Type read(ByteBuffer buffer) {
            boolean allowNulls = buffer.get() != 0;
            int size = buffer.getInt();
            AbstractMap.SimpleImmutableEntry[] entries = new AbstractMap.SimpleImmutableEntry[size];
            for (int i = 0; i < size; ++i) {
                String name = new StringType().read(buffer);
                Type type = Registry.INSTANCE.read(buffer);
                entries[i] = new AbstractMap.SimpleImmutableEntry<>(name, type);
            }
            return new StructureType(allowNulls, entries);
        }
    }

    public StructureType(boolean allowNulls, Map.Entry<String, Type> ...entries) {
        this.allowNulls = allowNulls;
        this.entries = entries;
    }

    public StructureType(Map.Entry<String, Type> ...entries) {
        this(true, entries);
    }

    public int getFieldIndex(String name) {
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            if (entry.getKey().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("Field <%s> not found for StructureType", name));
    }

    public Object getFieldValue(Object o, int i) {
        return ((Object[])o)[i];
    }

    public void setFieldValue(Object o, int i, Object v) {
        ((Object[])o)[i] = v;
    }

    public void setFieldValue(Object o, String name, Object v) {
        setFieldValue(o, getFieldIndex(name), v);
    }

    public Object getFieldValue(Object o, String name) {
        return getFieldValue(o, getFieldIndex(name));
    }

    public Object[] newInstance() {
        return new Object[entries.length];
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        Object[] structValue = (Object[]) value;
        assert structValue.length == entries.length;
        int size = 0;
        if (allowNulls) {
            BitSet nulls = Type.getNulls(structValue);
            size += VarbinaryType.INSTANCE.size(nulls.toByteArray());
        }
        for (int i = 0; i < entries.length; ++i) {
            if (!allowNulls) Objects.requireNonNull(structValue[i]);
            if (structValue[i] != null) {
                Map.Entry<String, Type> entry = entries[i];
                size += entry.getValue().size(structValue[i]);
            }
        }
        return size;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Object[] structValue = (Object[]) value;
        assert structValue.length == entries.length;
        if (allowNulls) {
            BitSet nulls = Type.getNulls(structValue);
            VarbinaryType.INSTANCE.write(buffer, nulls.toByteArray());
        }
        for (int i = 0; i < entries.length; ++i) {
            if (!allowNulls) Objects.requireNonNull(structValue[i]);
            if (structValue[i] != null) {
                Map.Entry<String, Type> entry = entries[i];
                entry.getValue().write(buffer, structValue[i]);
            }
        }
    }

    @Override
    public Object[] read(ByteBuffer buffer) {
        BitSet nulls = allowNulls ? BitSet.valueOf(VarbinaryType.INSTANCE.read(buffer)) : new BitSet();
        Object[] structValue =  new Object[entries.length];
        for (int i = 0; i < entries.length; ++i) {
            if (!nulls.get(i)) {
                Map.Entry<String, Type> entry = entries[i];
                Object value = entry.getValue().read(buffer);
                structValue[i] = value;
            }
        }
        return structValue;
    }

    @Override
    public Comparator<Object> comparator() {
        if (entries.length == 0) {
            return (o1, o2) -> 0;
        }
        Comparator<Object> comparator = Comparator.comparing(o -> ((Object[])o)[0], entries[0].getValue().comparator());
        for (int i = 1; i < entries.length; ++i) {
            int index = i;
            comparator = comparator.thenComparing(o -> ((Object[])o)[index], entries[index].getValue().comparator());
        }
        return comparator;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof StructureType))
            return false;
        return Objects.deepEquals(this.entries, ((StructureType)obj).entries);
    }
}
