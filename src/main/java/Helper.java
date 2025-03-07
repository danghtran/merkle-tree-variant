import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Helper {
    /***
     * Scenario: Standard Merkle Tree
     * Divide the file into chunks of 4KB and create a Merkle Tree to get the
     * Merkle Root.
     * A chunk is verified by sending the request to the server to build a
     * Merkle Tree
     */
    public static void main(String[] args) {
        try {
            byte[][] dataBytes = parseData("text.txt", 4096);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[][] hashBatch = hashData(dataBytes, md);
            byte[][][] tree = generateMerkleTree(hashBatch, md);
            byte[][][] proofList = new byte[hashBatch.length][][];
            for (int i = 0; i < hashBatch.length; i++) {
                proofList[i] = generateMerkleProof(hashBatch[i], tree);
            }
        } catch (IOException e) {
            System.out.println("Cannot read input file");
        } catch (NoSuchAlgorithmException ae) {
            System.out.println("SHA-256 not supported");
        }
    }

    /***
     * Parse data into uniform chunks
     */
    public static byte[][] parseData(String fileName, int chunkSize) throws IOException {
        File file = new File(fileName);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[][] dataBytes = new byte[(int) (file.length() / chunkSize)][chunkSize];
            for (int i = 0; i < dataBytes.length; i++) {
                fis.read(dataBytes[i], i * chunkSize, chunkSize);
            }
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

    public static boolean verify(String data, byte[][] proof, byte[] root, MessageDigest md) {
        byte[] hash = md.digest(strToBytes(data));
        byte[] proofRoot = genRootFromProof(hash, proof, md);
        return Arrays.equals(proofRoot, root);
    }

    public static byte[] genRootFromProof(byte[] hash, byte[][] proof, MessageDigest md) {
        byte[] result = hash;
        byte[] merged = new byte[64];
        for (int i = 0; i < proof.length; i++) {
            if (proof[i][32] == 1) {
                System.arraycopy(result, 0, merged, 0, 32);
                System.arraycopy(proof[i], 0, merged, 32, 32);
            } else {
                System.arraycopy(proof[i], 0, merged, 0, 32);
                System.arraycopy(result, 0, merged, 32, 32);
            }
            result = md.digest(merged);
        }
        return result;
    }

    public static byte[] getRootFromTree(byte[][][] tree) {
        return tree[tree.length - 1][0];
    }

    public static byte[][] generateMerkleProof(byte[] hash, byte[][][] tree) {
        byte[][] proof = new byte[tree.length - 1][33];
        int hashIndex = indexOf(hash, tree[0]);
        if (hashIndex != -1) {
            for (int i = 0; i < tree.length - 1; i++) {
                // level i
                int proofIndex = hashIndex + (int) Math.pow(-1, hashIndex % 2);
                System.arraycopy(tree[i][proofIndex], 0, proof[i], 0, 32);
                proof[i][32] = (byte) (proofIndex % 2);
                hashIndex = hashIndex / 2;
            }
        }
        return proof;
    }

    public static byte[][] generateMerkleProof(byte[] hash, byte[][] hashBatch, MessageDigest md) {
        int n = (int) (Math.log(hashBatch.length) / Math.log(2)) + 1;
        byte[][] proof = new byte[n - 1][33];
        int hashIndex = indexOf(hash, hashBatch);
        if (hashIndex != -1) {
            byte[][] hashToProcess = hashBatch;
            for (int i = 0; i < n - 1; i++) {
                // level i
                byte[][] lv = new byte[hashToProcess.length / 2][32];
                for (int j = 0; j < hashToProcess.length; j+=2) {
                    // pair j and j+1
                    byte[] merged = new byte[64];
                    System.arraycopy(hashToProcess[j], 0, merged, 0, 32);
                    System.arraycopy(hashToProcess[j + 1], 0, merged, 32, 32);
                    lv[j/2] = md.digest(merged);

                    if (j == hashIndex || j + 1 == hashIndex) {
                        System.arraycopy(hashToProcess[hashIndex != j ? j: j + 1], 0, proof[i], 0, 32);
                        proof[i][33] = (byte) ((hashIndex % 2) ^ 1);
                        hashIndex = hashIndex / 2;
                    }
                }
                hashToProcess = lv;
            }
        }
        return proof;
    }
    
    public static int indexOf(byte[] hash, byte[][] hashBatch) {
        for (int i = 0; i < hashBatch.length; i++) {
            if (Arrays.equals(hash, hashBatch[i])) {
                return i;
            }
        }
        return -1;
    }

    public static byte[][][] generateMerkleTree(byte[][] hashBatch, MessageDigest md) {
        int n = (int) (Math.log(hashBatch.length) / Math.log(2)) + 1;
        byte[][][] tree = new byte[n][][];
        tree[0] = hashBatch;
        for (int i = 1; i < n; i++) {
            // level i
            byte[][] hashToProcess = tree[i - 1];
            byte[][] lv = new byte[hashToProcess.length / 2][32];
            for (int j = 0; j < hashToProcess.length; j+=2) {
                // pair j and j+1
                byte[] merged = new byte[64];
                System.arraycopy(hashToProcess[j], 0, merged, 0, 32);
                System.arraycopy(hashToProcess[j + 1], 0, merged, 32, 32);
                lv[j/2] = md.digest(merged);
            }
            tree[i] = lv;
        }
        return tree;
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
}
