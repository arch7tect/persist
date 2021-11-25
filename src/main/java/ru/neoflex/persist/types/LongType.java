package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class LongType implements Type {
    public static final Type INSTANCE = new LongType();

    @Override
    public Value wrap(Object o) {
        return new LongValue((Long) o);
    }

    @Override
    public Object unwrap(Value value) {
        return ((LongValue)value).value;
    }

    @Override
    public int size(Value value) {
        return 8;
    }

    @Override
    public void write(ByteBuffer buffer, Value value) {
        buffer.putLong(((LongValue)value).value);
    }

    @Override
    public Value read(ByteBuffer buffer) {
        return new LongValue(buffer.getLong());
    }

    @Override
    public Comparator<Value> comparator() {
        return new Comparator<Value>() {
            @Override
            public int compare(Value o1, Value o2) {
                return Long.compare(((LongValue)o1).value, ((LongValue)o2).value);
            }
        };
    }
}
