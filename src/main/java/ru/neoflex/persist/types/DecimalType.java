package ru.neoflex.persist.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class DecimalType implements Type {
    public static final Type INSTANCE = new DecimalType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "decimal";
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
            return DecimalType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        BigDecimal decimal = (BigDecimal) value;
        return IntType.INSTANCE.size(decimal.scale()) + BigIntType.INSTANCE.size(decimal.unscaledValue());
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        BigDecimal decimal = (BigDecimal) value;
        IntType.INSTANCE.write(buffer, decimal.scale());
        BigIntType.INSTANCE.write(buffer, decimal.unscaledValue());
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int scale = (Integer) IntType.INSTANCE.read(buffer);
        BigInteger bi = (BigInteger) BigIntType.INSTANCE.read(buffer);
        return new BigDecimal(bi, scale);
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingDouble(value -> (Double) value);
    }
}
