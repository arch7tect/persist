package ru.neoflex.persist.types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class FloatType implements Type {
    public static final Type INSTANCE = new FloatType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "float";
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
            return FloatType.INSTANCE;
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
        buffer.putFloat((Float) value);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingDouble(value -> (Double) value);
    }
}
