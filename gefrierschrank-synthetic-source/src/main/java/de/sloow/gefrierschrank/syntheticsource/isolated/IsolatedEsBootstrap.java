package de.sloow.gefrierschrank.syntheticsource.isolated;

/**
 * Isolated-world twin of the ambient {@code EsBootstrap}: MUST live in this "isolated" subpackage
 * (see {@code IsolatedEsClassLoader}'s Javadoc), not share code with the ambient one, even though
 * the logic is identical. The reason is subtle but important: a class's constant-pool type
 * references resolve via that class's OWN defining classloader, regardless of which classloader
 * initiated the call into it. The ambient {@code EsBootstrap} is defined by the AMBIENT
 * classloader, so calling it - even from code running "inside" the isolated world - would wire up
 * the AMBIENT copy of {@code org.elasticsearch.logging.internal.spi.LoggerFactory}, not the
 * ISOLATED copy that {@link IsolatedReconstructionWorkerImpl} actually exercises (since
 * {@code IsolatedEsClassLoader} defines its own, separate {@code Class} objects for every
 * {@code org.elasticsearch.*}/{@code org.apache.lucene.*} type). Only a class that is ITSELF
 * defined by the isolated loader has type references that resolve against the isolated world's ES
 * classes - hence this duplicate, living in the isolated subpackage.
 */
final class IsolatedEsBootstrap {

    private static volatile boolean done = false;

    private IsolatedEsBootstrap() {}

    static synchronized void ensure() {
        if (done) {
            return;
        }
        org.elasticsearch.logging.internal.spi.LoggerFactory.setInstance(
            new org.elasticsearch.common.logging.internal.LoggerFactoryImpl());
        // Guards the IsolatedEsClassLoader holder substitution against ES renames: without it,
        // ES819 binary doc values would later fail with a NullPointerException deep inside ES.
        if (org.elasticsearch.nativeaccess.NativeAccess.instance().getZstd() == null) {
            throw new IllegalStateException(
                "isolated Elasticsearch zstd unavailable: NativeAccessHolder substitution did not apply");
        }
        // TermsEnum.<clinit> instantiates a BaseTermsEnum subclass (TermsEnum.EMPTY) while BaseTermsEnum
        // extends TermsEnum: a thread whose first contact is an empty Terms iterator and another whose
        // first contact is a terms dictionary (doc values, BlockTree) deadlock in class initialization.
        // Workers of concurrent shards and ranges enter leaves in different orders, so initialize the
        // pair once, here, before any worker exists.
        try {
            Class.forName("org.apache.lucene.index.TermsEnum", true, IsolatedEsBootstrap.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        done = true;
    }
}
