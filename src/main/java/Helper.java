import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class Helper {

    public static void main(String[] args) {
        // took almost 4200 ms for 2.000.000 chunks
        measureStandardMerkleTree("test.txt", 8);
    }

    public static void measureStandardMerkleTree(String inputFile, int chunkSize) {
        try {
            byte[][] dataBytes = parseData(inputFile, chunkSize);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Instant start = Instant.now();
            byte[][] hashBatch = hashData(dataBytes, md);
            byte[][][] tree = MerkleTree.generateMerkleTree(hashBatch, md);
            Instant end = Instant.now();
            System.out.println(Duration.between(start,end).toMillis());
        }
        catch (IOException e) {
            System.out.println("Cannot read input file");
        }
        catch (NoSuchAlgorithmException ae) {
            System.out.println("SHA-256 not supported");
        }
    }

    /***
     * Parse data into uniform chunks (ideally each row is a chunk)
     */
    public static byte[][] parseData(String fileName, int chunkSize) throws IOException {
        File file = new File(fileName);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[][] dataBytes = new byte[(int) (file.length() / chunkSize)][chunkSize];
            for (int i = 0; i < dataBytes.length; i++) {
                fis.read(dataBytes[i], 0, chunkSize);
            }
            fis.close();
            return dataBytes;
        }
    }

    public static byte[][] hashData(byte[][] dataChunks, MessageDigest md) {
        byte[][] hashBatch = new byte[dataChunks.length][32];
        for (int i = 0; i < dataChunks.length; i++) {
            hashBatch[i] = md.digest(dataChunks[i]);
        }
        return hashBatch;
    }

    public static byte[] strToBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

//    public static String encodeHexString(byte[] byteArray) {
//        StringBuilder hexStringBuffer = new StringBuilder();
//        for (byte b : byteArray) {
//            hexStringBuffer.append(byteToHex(b));
//        }
//        return hexStringBuffer.toString();
//    }
//
//    public static String byteToHex(byte num) {
//        char[] hexDigits = new char[2];
//        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
//        hexDigits[1] = Character.forDigit((num & 0xF), 16);
//        return new String(hexDigits);
//    }
//
    public static void createTestFile() {
        try (FileOutputStream fos = new FileOutputStream("test.txt")) {
            for (int i = 0; i < 2000000; i++) {
                fos.write("Text no\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
