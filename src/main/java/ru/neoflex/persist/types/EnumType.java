package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;

public class EnumType implements Type {
    Map.Entry<String, Type>[] entries;

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new EnumType.Super();

        @Override
        public String getName() {
            return "enum";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            EnumType enumType = (EnumType) value;
            int size = 4;
            for (Map.Entry<String, Type> entry: enumType.entries) {
                size += 4 + entry.getKey().getBytes(StandardCharsets.UTF_8).length;
                size += entry.getValue().getSuperType().size(entry.getValue());
            }
            return size;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            EnumType enumType = (EnumType) value;
            buffer.putInt(enumType.entries.length);
            for (Map.Entry<String, Type> entry: enumType.entries) {
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
            return new EnumType(entries);
        }
    }

    public EnumType(Map.Entry<String, Type> ...entries) {
        this.entries = entries;
    }

    private int findTypeIndex(String name) {
        for (int i = 0; i < entries.length; ++i) {
            Map.Entry<String, Type> entry = entries[i];
            if (entry.getKey().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException(String.format("Discriminator <%s> not found for EnumType", name));
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        Map.Entry<String, Object> enumValue = (Map.Entry<String, Object>) value;
        int i = findTypeIndex(enumValue.getKey());
        return 4 + entries[i].getValue().size(enumValue.getValue());
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Map.Entry<String, Object> enumValue = (Map.Entry<String, Object>) value;
        int i = findTypeIndex(enumValue.getKey());
        buffer.putInt(i);
        entries[i].getValue().write(buffer, enumValue.getValue());
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int i = buffer.getInt();
        return new AbstractMap.SimpleEntry<>(entries[i].getKey(), entries[i].getValue().read(buffer));
    }

    @Override
    public Comparator<Object> comparator() {
        return (o1, o2) -> {
            Map.Entry<String, Object> e1 = (Map.Entry<String, Object>) o1;
            int i1 = findTypeIndex(e1.getKey());
            Map.Entry<String, Object> e2 = (Map.Entry<String, Object>) o2;
            int i2 = findTypeIndex(e2.getKey());
            if (i1 != i2) {
                return i1 - 12;
            }
            return entries[i1].getValue().comparator().compare(e1.getValue(), e2.getValue());
        };
    }
}
