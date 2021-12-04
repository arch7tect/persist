package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class IntType implements Type {
    public static final Type INSTANCE = new IntType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "int";
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
            return IntType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 4;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        buffer.putInt((Integer) value);
    }

    @Override
    public Integer read(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingInt(value -> (Integer) value);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IntType;
    }
}
