package ru.neoflex.persist.types;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface SuperType extends Type {
    String getName();

    @Override
    default Comparator<Object> comparator() {
        return Comparator.comparing(o -> ((Type)o).getSuperType().getName());
    }
}
