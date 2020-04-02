package ru.neoflex.persist;

public interface IPage {
    int getSize();
    byte[] getData();
    void setData(byte[] data, boolean dirty);
    boolean isDirty();
    void setDirty(boolean dirty);
    byte[] read(int offset, int size);
    void write(int offset, byte[] data);
}
