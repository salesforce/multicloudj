package com.salesforce.multicloudj.common.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class HexUtil {

    private HexUtil() {}

    /**
     * Converts the hash string to bytes or empty array if decoding error occurs
     *
     * @param hash String representation of a hash
     * @return decoded hash
     */
    public static byte[] convertToBytes(String hash) {
        if (hash == null) {
            return new byte[0];
        }
        try {
            return Hex.decodeHex(hash);
        } catch (DecoderException e) {
            return new byte[0];
        }
    }
}
