package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

public class VarbinaryType implements Type {
    public static final VarbinaryType INSTANCE = new VarbinaryType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "varbinary";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            return 0;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
        }

        @Override
        public Type read(ByteBuffer buffer) {
            return VarbinaryType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 4 + ((byte[])value).length;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        byte[] nb = (byte[]) value;
        buffer.putInt(nb.length);
        buffer.put(nb);
    }

    @Override
    public byte[] read(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] nb = new byte[len];
        buffer.get(nb);
        return nb;
    }

    @Override
    public Comparator<Object> comparator() {
        return (o1, o2) ->  Arrays.compare((byte[]) o1, (byte[]) o2);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof VarbinaryType;
    }
}
