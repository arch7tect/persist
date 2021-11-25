package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class EmptyType implements Type {
    public static final Type INSTANCE = new LongType();

    @Override
    public byte tag() {
        return 1;
    }

    @Override
    public Value wrap(Object o) {
        return EmptyValue.INSTANCE;
    }

    @Override
    public Object unwrap(Value value) {
        return EmptyValue.VALUE;
    }

    @Override
    public int size(Value value) {
        return 0;
    }

    @Override
    public void write(ByteBuffer buffer, Value value) {

    }

    @Override
    public Value read(ByteBuffer buffer) {
        return EmptyValue.INSTANCE;
    }

    @Override
    public Comparator<Value> comparator() {
        return (o1, o2) -> 0;
    }
}
