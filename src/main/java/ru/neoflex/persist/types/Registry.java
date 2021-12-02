package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

public class Registry implements SuperType {
    public static final Registry INSTANCE = new Registry();

    public static final EnumType TYPES = new EnumType(
            new AbstractMap.SimpleImmutableEntry<>(INSTANCE.getName(), INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(EmptyType.Super.INSTANCE.getName(), EmptyType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(ShortType.Super.INSTANCE.getName(), ShortType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(IntType.Super.INSTANCE.getName(), IntType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(LongType.Super.INSTANCE.getName(), LongType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(BigIntType.Super.INSTANCE.getName(), BigIntType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(FloatType.Super.INSTANCE.getName(), FloatType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(DoubleType.Super.INSTANCE.getName(), DoubleType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(DecimalType.Super.INSTANCE.getName(), DecimalType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(StringType.Super.INSTANCE.getName(), StringType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(VarbinaryType.Super.INSTANCE.getName(), VarbinaryType.Super.INSTANCE),
            new AbstractMap.SimpleImmutableEntry<>(BinaryType.Super.INSTANCE.getName(), BinaryType.Super.INSTANCE),
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
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Type type = (Type) value;
        String name = type.getSuperType().getName();
        TYPES.write(buffer, new AbstractMap.SimpleEntry<>(name, type));
    }

    @Override
    public Type read(ByteBuffer buffer) {
        Map.Entry<String, Type> entry = (Map.Entry<String, Type>) TYPES.read(buffer);
        return entry.getValue();
    }
}
