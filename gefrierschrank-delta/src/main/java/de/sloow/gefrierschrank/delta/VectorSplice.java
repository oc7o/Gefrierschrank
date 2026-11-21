package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.Set;

/**
 * Puts rehydrated root-level vector members into a stored JSON object without touching any stored
 * byte: the members go before the object's final brace, where Elasticsearch's own patch appends
 * them ({@code LinkedHashMap.put} of a new root key). Anything that is not one JSON object whose
 * root holds none of the keys is left to Elasticsearch's reserialization ({@code null}). The result
 * is validated again on the row path like any stored source. One instance per thread.
 */
final class VectorSplice {

    private final JsonFactory json = new JsonFactory();

    /**
     * {@code stored[0, length)} with {@code members} ({@code "k":v,...}, no braces) appended inside
     * its root object, or null when that cannot be done byte-preservingly.
     */
    byte[] splice(byte[] stored, int length, String[] keys, byte[] members) {
        int open = 0;
        while (open < length && isWhitespace(stored[open])) open++;
        int close = length - 1;
        while (close > open && isWhitespace(stored[close])) close--;
        if (open >= close || stored[open] != '{' || stored[close] != '}') return null;
        if (!rootObjectLacks(stored, length, Set.of(keys))) return null;
        int inside = open + 1;
        while (inside < close && isWhitespace(stored[inside])) inside++;
        boolean empty = inside == close;

        byte[] out = new byte[length + (empty ? 0 : 1) + members.length];
        System.arraycopy(stored, 0, out, 0, close);
        int at = close;
        if (!empty) out[at++] = ',';
        System.arraycopy(members, 0, out, at, members.length);
        System.arraycopy(stored, close, out, at + members.length, length - close);
        return out;
    }

    /** One JSON object and nothing after it, none of whose root keys is in {@code keys}. */
    private boolean rootObjectLacks(byte[] stored, int length, Set<String> keys) {
        try (JsonParser parser = json.createParser(stored, 0, length)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) return false;
            for (JsonToken token = parser.nextToken(); token != JsonToken.END_OBJECT; token = parser.nextToken()) {
                if (token != JsonToken.FIELD_NAME || keys.contains(parser.currentName())) return false;
                parser.nextToken();
                parser.skipChildren();
            }
            return parser.nextToken() == null;
        } catch (IOException | RuntimeException malformed) {
            return false;
        }
    }

    private static boolean isWhitespace(byte b) {
        return b == ' ' || b == '\t' || b == '\n' || b == '\r';
    }
}
