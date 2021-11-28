package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface Type {
    SuperType getSuperType();
    int size(Object value);
    void write(ByteBuffer buffer, Object value);
    Object read(ByteBuffer buffer);
    Comparator<Object> comparator();
 }
