package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TypeValue implements Type, Value {
    private static Map<Integer, Function<Value, Type>> factories = new HashMap<>();
    private Type type;

    public TypeValue(Type type) {
        this.type = type;
    }

    @Override
    public byte tag() {
        return 0;
    }

    @Override
    public Value wrap(Object o) {
        return new TypeValue((Type) o);
    }

    @Override
    public Object unwrap(Value value) {
        return ((TypeValue)value).type;
    }

    @Override
    public int size(Value value) {
        return 1 + ((TypeValue)value).type.payload().size();
    }

    @Override
    public void write(ByteBuffer buffer, Value value) {
        TypeValue typeValue = (TypeValue)value;
        buffer.put(typeValue.type.tag());
        Value payload = typeValue.type.payload();
        wrap(payload.getType()).write(buffer);
        payload.write(buffer);
    }

    @Override
    public Value read(ByteBuffer buffer) {
        return null;
    }

    @Override
    public Comparator<Value> comparator() {
        return null;
    }

    @Override
    public Type getType() {
        return null;
    }
}
