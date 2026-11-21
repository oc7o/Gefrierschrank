package de.sloow.gefrierschrank.syntheticsource;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Class-file bytes that {@link IsolatedEsClassLoader} defines as
 * {@code org.elasticsearch.nativeaccess.NativeAccessHolder} in the isolated world, equivalent to
 *
 * <pre>
 * final class NativeAccessHolder {
 *     static final NativeAccess INSTANCE = new IsolatedJniNativeAccess();
 * }
 * </pre>
 *
 * <p>ES's own holder hard-wires its static initializer to probe for ES's bundled libzstd and falls
 * back to a {@code NoopNativeAccess} without zstd; {@code NativeAccess.instance()} is nothing but
 * {@code getstatic NativeAccessHolder.INSTANCE}. Emitting the replacement here rather than compiling
 * a same-named source file keeps it out of every class path and out of the shaded CLI jar, where a
 * second {@code NativeAccessHolder.class} would shadow ES's class for the ambient world depending on
 * entry order. Class-file version 52 with a branch-free {@code <clinit>} needs no StackMapTable,
 * and the JVM verifies the bytes when they are defined.
 */
final class IsolatedNativeAccessHolder {

    static final String CLASS_NAME = "org.elasticsearch.nativeaccess.NativeAccessHolder";

    private static final byte[] BYTES = emit("org/elasticsearch/nativeaccess/NativeAccessHolder",
        "org/elasticsearch/nativeaccess/IsolatedJniNativeAccess");

    private IsolatedNativeAccessHolder() {}

    static byte[] bytes() {
        return BYTES.clone();
    }

    private static byte[] emit(String holder, String implementation) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(0xCAFEBABE);
            out.writeShort(0);                                            // minor version
            out.writeShort(52);                                           // major version (Java 8)
            out.writeShort(17);                                           // constant pool count
            utf8(out, holder);                                            // #1
            classRef(out, 1);                                             // #2 this class
            utf8(out, "java/lang/Object");                                // #3
            classRef(out, 3);                                             // #4 super class
            utf8(out, "INSTANCE");                                        // #5
            utf8(out, "Lorg/elasticsearch/nativeaccess/NativeAccess;");   // #6
            utf8(out, implementation);                                    // #7
            classRef(out, 7);                                             // #8
            utf8(out, "<init>");                                          // #9
            utf8(out, "()V");                                             // #10
            nameAndType(out, 9, 10);                                      // #11
            memberRef(out, 10, 8, 11);                                    // #12 Methodref implementation.<init>
            nameAndType(out, 5, 6);                                       // #13
            memberRef(out, 9, 2, 13);                                     // #14 Fieldref INSTANCE
            utf8(out, "<clinit>");                                        // #15
            utf8(out, "Code");                                            // #16
            out.writeShort(0x0030);                                       // ACC_FINAL | ACC_SUPER, package-private
            out.writeShort(2);
            out.writeShort(4);
            out.writeShort(0);                                            // interfaces
            out.writeShort(1);                                            // fields
            out.writeShort(0x0018);                                       // static final, package-private
            out.writeShort(5);
            out.writeShort(6);
            out.writeShort(0);
            out.writeShort(1);                                            // methods
            out.writeShort(0x0008);                                       // static
            out.writeShort(15);
            out.writeShort(10);
            out.writeShort(1);                                            // one attribute: Code
            byte[] code = {
                (byte) 0xbb, 0, 8,                                        // new #8
                0x59,                                                     // dup
                (byte) 0xb7, 0, 12,                                       // invokespecial #12
                (byte) 0xb3, 0, 14,                                       // putstatic #14
                (byte) 0xb1                                               // return
            };
            out.writeShort(16);
            out.writeInt(2 + 2 + 4 + code.length + 2 + 2);
            out.writeShort(2);                                            // max stack
            out.writeShort(0);                                            // max locals
            out.writeInt(code.length);
            out.write(code);
            out.writeShort(0);                                            // exception table
            out.writeShort(0);                                            // code attributes
            out.writeShort(0);                                            // class attributes
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void utf8(DataOutputStream out, String value) throws IOException {
        out.writeByte(1);
        out.writeUTF(value);
    }

    private static void classRef(DataOutputStream out, int nameIndex) throws IOException {
        out.writeByte(7);
        out.writeShort(nameIndex);
    }

    private static void nameAndType(DataOutputStream out, int nameIndex, int descriptorIndex) throws IOException {
        out.writeByte(12);
        out.writeShort(nameIndex);
        out.writeShort(descriptorIndex);
    }

    private static void memberRef(DataOutputStream out, int tag, int classIndex, int nameAndTypeIndex) throws IOException {
        out.writeByte(tag);
        out.writeShort(classIndex);
        out.writeShort(nameAndTypeIndex);
    }
}
