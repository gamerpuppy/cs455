package cs455.scaling.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA {

    public static String SHA1FromBytes(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA1");
        byte[] hash = digest.digest(data);
        BigInteger hashInt = new BigInteger(1, hash);

        return hashInt.toString(16);
    }

    public static String SHA1FromBytesPadded(byte[] data, int desiredSize){
        try {
            String hash = SHA.SHA1FromBytes(data);
            String pad = "";
            for (int i = hash.length(); i < desiredSize; i++) {
                pad += '0';
            }
            return pad + hash;

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
            return "";
        }
    }

}
