package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

public class Registry implements SuperType {
    public static final Registry INSTANCE = new Registry();
    {
        INSTANCE.registerType(INSTANCE);
        INSTANCE.registerType(EmptyType.Super.INSTANCE);
        INSTANCE.registerType(LongType.Super.INSTANCE);
    }

    private final Map<String, SuperType> types = new HashMap<>();

    public void registerType(SuperType superType) {
        types.put(superType.getName(), superType);
    }

    @Override
    public String getName() {
        return "type";
    }

    @Override
    public SuperType getSuperType() {
        return this;
    }

    @Override
    public int size(Object value) {
        Type type = (Type) value;
        String name = type.getSuperType().getName();
        return 4 + name.getBytes(StandardCharsets.UTF_8).length + type.getSuperType().size(type);
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Type type = (Type) value;
        String name = type.getSuperType().getName();
        byte[] nb = name.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(nb.length);
        buffer.get(nb);
        type.getSuperType().write(buffer, type);
    }

    @Override
    public Type read(ByteBuffer buffer) {
        int size = buffer.getInt();
        byte[] nb = new byte[size];
        buffer.get(nb);
        String name = new String(nb, StandardCharsets.UTF_8);
        SuperType superType = types.get(name);
        if (superType == null) {
            throw new IllegalArgumentException(String.format("Type <%s> not found", name));
        }
        return (Type) superType.read(buffer);
    }
}
