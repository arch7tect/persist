package ru.neoflex.persist.types;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class BigIntType implements Type {
    public static final Type INSTANCE = new BigIntType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "bigint";
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
            return BigIntType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        BigInteger bi = (BigInteger) value;
        return bi.bitLength()/8 + 1;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        BigInteger bi = (BigInteger) value;
        VarbinaryType.INSTANCE.write(buffer, bi.toByteArray());
    }

    @Override
    public Object read(ByteBuffer buffer) {
        return new BigInteger((byte[])VarbinaryType.INSTANCE.read(buffer));
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparingLong(value -> (Long)value);
    }
}
