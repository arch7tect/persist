package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.*;

public class ArrayType implements Type {
    boolean allowNulls;
    Type elementType;

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new ArrayType.Super();

        @Override
        public String getName() {
            return "array";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            ArrayType arrayType = (ArrayType) value;
            return 1 + Registry.INSTANCE.size(arrayType.elementType);
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            ArrayType arrayType = (ArrayType) value;
            buffer.put((byte) (arrayType.allowNulls ? 1 : 0));
            Registry.INSTANCE.write(buffer, arrayType.elementType);
        }

        @Override
        public Type read(ByteBuffer buffer) {
            boolean allowNulls = buffer.get() != 0;
            Type elementType = Registry.INSTANCE.read(buffer);
            return new ArrayType(allowNulls, elementType);
        }

        @Override
        public Comparator<Object> comparator() {
            return SuperType.super.comparator().thenComparing(o -> ((ArrayType)o).elementType, SuperType.super.comparator());
        }
    }

    public ArrayType(boolean allowNulls, Type elementType) {
        this.allowNulls = allowNulls;
        this.elementType = elementType;
    }

    public ArrayType(Type elementType) {
        this(true, elementType);
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        Object[] arrayValue = (Object[]) value;
        int size = 4;
        if (allowNulls) {
            BitSet nulls = Type.getNulls(arrayValue);
            size += VarbinaryType.INSTANCE.size(nulls.toByteArray());
        }
        for (int i = 0; i < arrayValue.length; ++i) {
            if (!allowNulls) Objects.requireNonNull(arrayValue[i]);
            if (arrayValue[i] != null) {
                size += elementType.size(arrayValue[i]);
            }
        }
        return size;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        Object[] arrayValue = (Object[]) value;
        buffer.putInt(arrayValue.length);
        if (allowNulls) {
            BitSet nulls = Type.getNulls(arrayValue);
            VarbinaryType.INSTANCE.write(buffer, nulls.toByteArray());
        }
        for (int i = 0; i < arrayValue.length; ++i) {
            if (!allowNulls) Objects.requireNonNull(arrayValue[i]);
            if (arrayValue[i] != null) {
                elementType.write(buffer, arrayValue[i]);
            }
        }
    }

    @Override
    public Object[] read(ByteBuffer buffer) {
        int len = buffer.getInt();
        Object[] arrayValue =  new Object[len];
        BitSet nulls = allowNulls ? BitSet.valueOf(VarbinaryType.INSTANCE.read(buffer)) : new BitSet();
        for (int i = 0; i < len; ++i) {
            if (!nulls.get(i)) {
                arrayValue[i] = elementType.read(buffer);
            }
        }
        return arrayValue;
    }

    @Override
    public Comparator<Object> comparator() {
        return (o1, o2) ->  Arrays.compare((Object[]) o1, (Object[]) o2, elementType.comparator());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ArrayType))
            return false;
        return this.elementType.equals(((ArrayType)obj).elementType);
    }
}
