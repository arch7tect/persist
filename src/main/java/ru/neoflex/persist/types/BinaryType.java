package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class BinaryType implements Type {
    int length;

    public BinaryType(int length) {
        this.length = length;
    }

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "binary";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            return 4;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            buffer.putInt(((BinaryType)value).length);
        }

        @Override
        public Type read(ByteBuffer buffer) {
            BinaryType t = new BinaryType(0);
            t.length = buffer.getInt();
            return t;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return ((byte[])value).length;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        byte[] nb = (byte[]) value;
        assert nb.length == length;
        buffer.put(nb);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        byte[] nb = new byte[length];
        buffer.get(nb);
        return nb;
    }

    @Override
    public Comparator<Object> comparator() {
        return (o1, o2) ->  Arrays.compare((byte[]) o1, (byte[]) o2);
    }
}
