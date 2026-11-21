package de.sloow.gefrierschrank.cli;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.utility.JavaModule;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.nameStartsWith;

/**
 * The CLI's {@link Main}, run in a child JVM started with {@code -javaagent:<byte-buddy-agent jar>},
 * with an OutOfMemoryError injected where a real one can strike: entering a method, JDK methods
 * such as a queue's {@code offer} included.
 *
 * <p>Each {@code -Dinject.<type>.<method>=<thread prefix>|<argument class>|<nth>} property throws
 * one at the {@code nth} entry (0: every entry) by a thread whose name starts with the prefix and,
 * if an argument class is given, whose first argument is exactly of that class. Every such type is
 * loaded and instrumented before Main starts, and "instrumented <type>" printed to stderr.
 */
public final class HandOffFailureChild {

    static final String PREFIX = "inject.";

    private HandOffFailureChild() {}

    /** Inlined into the target method, also into java.base classes: it may use only what they see. */
    public static final class Inject {
        @Advice.OnMethodEnter
        static void enter(@Advice.Origin("#t.#m") String origin, @Advice.Argument(value = 0, optional = true) Object argument) {
            String spec = System.getProperty(PREFIX.concat(origin));
            if (spec == null) return;
            int first = spec.indexOf('|');
            int second = spec.indexOf('|', first + 1);
            if (!Thread.currentThread().getName().startsWith(spec.substring(0, first))) return;
            String argumentClass = spec.substring(first + 1, second);
            if (!argumentClass.isEmpty() && (argument == null || !argument.getClass().getName().equals(argumentClass))) return;
            int nth = Integer.parseInt(spec.substring(second + 1));
            synchronized (System.class) {
                String counter = "inject-count.".concat(origin);
                int count = Integer.parseInt(System.getProperty(counter, "0")) + 1;
                System.setProperty(counter, Integer.toString(count));
                if (nth != 0 && count != nth) return;
            }
            throw new OutOfMemoryError("injected at ".concat(origin));
        }
    }

    public static void main(String[] args) throws Exception {
        AgentBuilder agent = new AgentBuilder.Default()
            .disableClassFormatChanges()
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .with(new AgentBuilder.Listener.Adapter() {
                @Override
                public void onTransformation(TypeDescription type, ClassLoader loader, JavaModule module, boolean loaded,
                                             DynamicType dynamicType) {
                    System.err.println("instrumented " + type.getName());
                }

                @Override
                public void onError(String type, ClassLoader loader, JavaModule module, boolean loaded, Throwable error) {
                    System.err.println("cannot instrument " + type + ": " + error);
                }
            })
            .ignore(nameStartsWith("net.bytebuddy."));
        for (String property : System.getProperties().stringPropertyNames()) {
            if (!property.startsWith(PREFIX)) continue;
            String target = property.substring(PREFIX.length());
            String type = target.substring(0, target.lastIndexOf('.'));
            String method = target.substring(target.lastIndexOf('.') + 1);
            Class.forName(type, false, HandOffFailureChild.class.getClassLoader());
            agent = agent.type(named(type))
                .transform((builder, description, loader, module, domain) -> builder.visit(Advice.to(Inject.class).on(named(method))));
        }
        agent.installOn(ByteBuddyAgent.getInstrumentation());
        Main.main(args);
    }
}
