package ru.neoflex.persist.types;

public class LongValue implements Value {
    long value;

    public LongValue(Long value) {
        this.value = value;
    }

    @Override
    public Type getType() {
        return LongType.INSTANCE;
    }
}
