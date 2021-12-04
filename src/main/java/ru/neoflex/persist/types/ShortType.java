package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ShortType implements Type {
    public static final Type INSTANCE = new ShortType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "short";
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
            return ShortType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 2;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        buffer.putShort((Short) value);
    }

    @Override
    public Short read(ByteBuffer buffer) {
        return buffer.getShort();
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingInt(value -> (Short) value);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ShortType;
    }
}
