package com.salesforce.multicloudj.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class HexUtilTest {

    @Test
    void testConvert() {
        byte[] expected = {93, 65, 64, 42, -68, 75, 42, 118, -71, 113, -99, -111, 16, 23, -59, -110};
        byte[] actual = HexUtil.convertToBytes("5d41402abc4b2a76b9719d911017c592");
        assertArrayEquals(expected, actual);
    }

    @Test
    void testBadInput() {
        byte[] actual = HexUtil.convertToBytes("5d41402abc4b2a76b9719d9");
        assertArrayEquals(new byte[0], actual);
    }

    @Test
    void testNullInput() {
        byte[] actual = HexUtil.convertToBytes("5d41402abc4b2a76b9719d9");
        assertArrayEquals(new byte[0], actual);
    }
}
