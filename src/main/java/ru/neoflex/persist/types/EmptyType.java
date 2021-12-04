package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class EmptyType implements Type {
    public static final Type INSTANCE = new EmptyType();

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new EmptyType.Super();

        @Override
        public String getName() {
            return "empty";
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
            return EmptyType.INSTANCE;
        }
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 0;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {

    }

    @Override
    public Object read(ByteBuffer buffer) {
        return INSTANCE;
    }

    @Override
    public Comparator<Object> comparator() {
        return (o1, o2) -> 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof EmptyType;
    }
}
