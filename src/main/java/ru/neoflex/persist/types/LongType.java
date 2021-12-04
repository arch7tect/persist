package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class LongType implements Type {
    public static final Type INSTANCE = new LongType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "long";
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
            return LongType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 8;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        buffer.putLong((Long) value);
    }

    @Override
    public Long read(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingLong(value -> (Long)value);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LongType;
    }
}
