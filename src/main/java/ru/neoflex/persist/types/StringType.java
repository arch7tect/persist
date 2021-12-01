package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class StringType implements Type {
    private boolean ignoreCase = false;

    public static class Super implements SuperType {
        public static final SuperType INSTANCE = new Super();

        @Override
        public String getName() {
            return "string";
        }

        @Override
        public SuperType getSuperType() {
            return Registry.INSTANCE;
        }

        @Override
        public int size(Object value) {
            return 1;
        }

        @Override
        public void write(ByteBuffer buffer, Object value) {
            StringType t = (StringType) value;
            buffer.put((byte) (t.ignoreCase?1:0));
        }

        @Override
        public Type read(ByteBuffer buffer) {
            boolean ignoreCase = buffer.get() != 0;
            return new StringType(ignoreCase);
        }
    }

    public StringType(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public StringType() {
        this(false);
    }

    @Override
    public SuperType getSuperType() {
        return Super.INSTANCE;
    }

    @Override
    public int size(Object value) {
        return 4 + value.toString().getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public void write(ByteBuffer buffer, Object value) {
        byte[] nb = value.toString().getBytes(StandardCharsets.UTF_8);
        buffer.putInt(nb.length);
        buffer.put(nb);
    }

    @Override
    public Object read(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] nb = new byte[len];
        buffer.get(nb);
        return new String(nb, StandardCharsets.UTF_8);
    }

    @Override
    public Comparator<Object> comparator() {
        return Comparator.comparing(Object::toString);
    }
}
