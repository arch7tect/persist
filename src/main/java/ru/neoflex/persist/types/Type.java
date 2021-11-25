package ru.neoflex.persist.types;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Comparator;

public interface Type {
    byte tag();
    Value wrap(Object o);
    Object unwrap(Value value);
    int size(Value value);
    void write(ByteBuffer buffer, Value value);
    Value read(ByteBuffer buffer);
    Comparator<Value> comparator();
    default Value payload() {
        return EmptyValue.INSTANCE;
    }
}
