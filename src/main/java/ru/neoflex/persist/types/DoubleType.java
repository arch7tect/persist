package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class DoubleType implements Type {
    public static final Type INSTANCE = new DoubleType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "double";
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
            return DoubleType.INSTANCE;
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
        buffer.putDouble((Double) value);
    }

    @Override
    public Double read(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingDouble(value -> (Double) value);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DoubleType;
    }
}
