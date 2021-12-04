package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Objects;

public interface Type {
    SuperType getSuperType();
    int size(Object value);
    void write(ByteBuffer buffer, Object value);

    static BitSet getNulls(Object[] arr) {
        BitSet nulls = new BitSet(arr.length);
        for (int i = 0; i < arr.length; ++i) {
            if (arr[i] == null) {
                nulls.set(i);
            }
        }
        return nulls;
    }

    Object read(ByteBuffer buffer);
    Comparator<Object> comparator();
 }
