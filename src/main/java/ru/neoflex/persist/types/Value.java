package ru.neoflex.persist.types;

import java.nio.ByteBuffer;

public interface Value {
    Type getType();

    default Object unwrap() {
        return getType().unwrap(this);
    }

    default int size() {
        return getType().size(this);
    }

    default void write(ByteBuffer buffer) {
        getType().write(buffer, this);
    }
}
