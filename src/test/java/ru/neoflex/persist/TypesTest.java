package ru.neoflex.persist;

import org.junit.Assert;
import org.junit.Test;
import ru.neoflex.persist.types.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;

public class TypesTest {
    @Test
    public void simpleTypesTest() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        Integer i0 = 12345;
        IntType.INSTANCE.write(buffer, i0);
        Integer i1 = (Integer) IntType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(i0, i1);

        Long l0 = 12345L;
        LongType.INSTANCE.write(buffer.rewind(), l0);
        Long l1 = (Long) LongType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(l0, l1);

        Short s0 = 12345;
        ShortType.INSTANCE.write(buffer.rewind(), s0);
        Short s1 = (Short) ShortType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(s0, s1);

        BigInteger b0 = new BigInteger("1234567890123456789012345678901234567890");
        BigIntType.INSTANCE.write(buffer.rewind(), b0);
        BigInteger b1 = (BigInteger) BigIntType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(b0, b1);

        Float f0 = 12345.1234f;
        FloatType.INSTANCE.write(buffer.rewind(), f0);
        Float f1 = (Float) FloatType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(f0, f1);

        Double d0 = 12345.1234;
        DoubleType.INSTANCE.write(buffer.rewind(), d0);
        Double d1 = (Double) DoubleType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(d0, d1);

        BigDecimal bd0 = new BigDecimal("1234567890123456789012345678901234567890.123456789012345678901");
        DecimalType.INSTANCE.write(buffer.rewind(), bd0);
        BigDecimal bd1 = (BigDecimal) DecimalType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(bd0, bd1);

        String st0 = "qwertyuiopasdfghjkl;'";
        StringType.INSTANCE.write(buffer.rewind(), st0);
        String st1 = (String) StringType.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(st0, st1);

        byte[] v0 = "qwertyuiopasdfghjkl;'".getBytes(StandardCharsets.UTF_8);
        VarbinaryType.INSTANCE.write(buffer.rewind(), v0);
        byte[] v1 = (byte[]) VarbinaryType.INSTANCE.read(buffer.rewind());
        Assert.assertArrayEquals(v0, v1);

        BinaryType bt = new BinaryType(v0.length);
        bt.write(buffer.rewind(), v0);
        byte[] v2 = (byte[]) bt.read(buffer.rewind());
        Assert.assertArrayEquals(v0, v2);
    }

    @Test
    public void complexTypesTest() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        StructureType st = new StructureType(
                new AbstractMap.SimpleEntry<>("id", IntType.INSTANCE),
                new AbstractMap.SimpleEntry<>("name", StringType.INSTANCE),
                new AbstractMap.SimpleEntry<>("last_name", StringType.INSTANCE),
                new AbstractMap.SimpleEntry<>("sex", new EnumType(
                        new AbstractMap.SimpleEntry<>("M", EmptyType.INSTANCE),
                        new AbstractMap.SimpleEntry<>("F", EmptyType.INSTANCE)
                )),
                new AbstractMap.SimpleEntry<>("salary", DecimalType.INSTANCE),
                new AbstractMap.SimpleEntry<>("groups", new ArrayType(StringType.INSTANCE))
        );

        Registry.INSTANCE.write(buffer.rewind(), st);
        Assert.assertEquals(Registry.INSTANCE.size(st), buffer.position());
        Type st2 = Registry.INSTANCE.read(buffer.rewind());
        Assert.assertEquals(st, st2);

        Object[] u0 = st.newInstance();
        st.setFieldValue(u0, "id", 100);
        st.setFieldValue(u0, "name", "Oleg");
        st.setFieldValue(u0, "sex", EnumType.newInstance("M", EmptyType.INSTANCE));
        st.setFieldValue(u0, "salary", new BigDecimal("12345.67"));
        st.setFieldValue(u0, "groups", new Object[] {"Users", null, "Admins"});
        st.write(buffer.rewind(), u0);
        Assert.assertEquals(st.size(u0), buffer.position());
        Object[] u1 = st.read(buffer.rewind());
        Assert.assertArrayEquals(u0, u1);
    }
}
