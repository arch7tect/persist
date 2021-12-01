package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

public class Registry implements SuperType {
    public static final Registry INSTANCE = new Registry();
//    {
//        INSTANCE.registerType(INSTANCE);
//        INSTANCE.registerType(EmptyType.Super.INSTANCE);
//        INSTANCE.registerType(LongType.Super.INSTANCE);
//    }
//    private final Map<String, SuperType> types = new HashMap<>();
//
//    public void registerType(SuperType superType) {
//        types.put(superType.getName(), superType);
//    }

    public static final EnumType TYPES = new EnumType(
            new AbstractMap.SimpleImmutableEntry<>(INSTANCE.getName(), INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(EmptyType.Super.INSTANCE.getName(), EmptyType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(LongType.Super.INSTANCE.getName(), LongType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(StringType.Super.INSTANCE.getName(), StringType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(VarbinaryType.Super.INSTANCE.getName(), VarbinaryType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(EnumType.Super.INSTANCE.getName(), EnumType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(StructureType.Super.INSTANCE.getName(), StructureType.Super.INSTANCE)
    );

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
        return TYPES.size(new AbstractMap.SimpleEntry<>(name, type));
//        return 4 + name.getBytes(StandardCharsets.UTF_8).length + type.getSuperType().size(type);
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Type type = (Type) value;
        String name = type.getSuperType().getName();
        TYPES.write(buffer, new AbstractMap.SimpleEntry<>(name, type));
//        byte[] nb = name.getBytes(StandardCharsets.UTF_8);
//        buffer.putInt(nb.length);
//        buffer.put(nb);
//        type.getSuperType().write(buffer, type);
    }

    @Override
    public Type read(ByteBuffer buffer) {
        Map.Entry<String, Type> entry = (Map.Entry<String, Type>) TYPES.read(buffer);
        return entry.getValue();
//        int size = buffer.getInt();
//        byte[] nb = new byte[size];
//        buffer.get(nb);
//        String name = new String(nb, StandardCharsets.UTF_8);
//        SuperType superType = types.get(name);
//        if (superType == null) {
//            throw new IllegalArgumentException(String.format("Type <%s> not found", name));
//        }
//        return (Type) superType.read(buffer);
    }
}
