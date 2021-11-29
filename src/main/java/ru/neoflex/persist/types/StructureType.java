package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;

public class StructureType implements Type {
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
            int size = 4;
            for (Map.Entry<String, Type> entry: structureType.entries) {
                size += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
                size += entry.getValue().getSuperType().size(entry.getValue());
            }
            return size;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            StructureType structureType = (StructureType) value;
            buffer.putInt(structureType.entries.length);
            for (Map.Entry<String, Type> entry: structureType.entries) {
                String name = entry.getKey();
                byte[] nb = name.getBytes(StandardCharsets.UTF_8);
                buffer.putInt(nb.length);
                buffer.put(nb);
                entry.getValue().getSuperType().write(buffer, entry.getValue());
            }
        }

        @Override
        public Type read(ByteBuffer buffer) {
            int size = buffer.getInt();
            AbstractMap.SimpleImmutableEntry[] entries = new AbstractMap.SimpleImmutableEntry[size];
            for (int i = 0; i < size; ++i) {
                int len = buffer.getInt();
                byte[] nb = new byte[len];
                buffer.get(nb);
                String name = new String(nb, StandardCharsets.UTF_8);
                Type type = Registry.INSTANCE.read(buffer);
                entries[i] = new AbstractMap.SimpleImmutableEntry<>(name, type);
            }
            return new StructureType(entries);
        }
    }

    public StructureType(Map.Entry<String, Type> ...entries) {
        this.entries = entries;
    }

    private int findTypeIndex(String name) {
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            if (entry.getKey().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("Field <%s> not found for EnumType", name));
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
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            size += entry.getValue().size(structValue[i]);
        }
        return size;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Object[] structValue = (Object[]) value;
        assert structValue.length == entries.length;
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            entry.getValue().write(buffer, structValue[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buffer) {
        Object[] structValue =  new Object[entries.length];
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            Object value = entry.getValue().read(buffer);
            structValue[i] = value;
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
}
