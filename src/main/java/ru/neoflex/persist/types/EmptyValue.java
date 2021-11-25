package ru.neoflex.persist.types;

public class EmptyValue implements Value {
    public static final Value INSTANCE = new EmptyValue();
    public static final Object VALUE = new Object();
    @Override
    public Type getType() {
        return EmptyType.INSTANCE;
    }
}
