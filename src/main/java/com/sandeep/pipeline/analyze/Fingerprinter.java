package com.sandeep.pipeline.analyze;

import com.sandeep.pipeline.parse.LogEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;

/**
 * Stage 2 — Fingerprinting (per-event, stateless, thread-safe). Produces a stable hash identifying
 * the <em>kind</em> of event so thousands of occurrences of one root cause group into a single issue.
 * Variable parts of a stack trace (line numbers, hex addresses, object ids, lambda suffixes) are
 * stripped before hashing.
 *
 * <p>Note: long digit runs are normalized, so e.g. {@code HTTP 500} and {@code HTTP 503} fingerprint
 * identically. That is intentional for root-cause grouping; classification/category fields remain
 * available downstream to distinguish them when needed.
 */
public class Fingerprinter {

    private static final Pattern LINE_NUMBERS = Pattern.compile(":\\d+");
    private static final Pattern HEX_ADDRS    = Pattern.compile("0x[0-9a-fA-F]+");
    private static final Pattern LAMBDA_HASH  = Pattern.compile("\\$\\$Lambda\\$\\d+/0x[0-9a-fA-F]+");
    private static final Pattern OBJ_ID       = Pattern.compile("@[0-9a-fA-F]+");
    private static final Pattern DIGITS_RUN   = Pattern.compile("\\d{2,}");
    private static final Pattern WS           = Pattern.compile("\\s+");
    private static final int MAX_FRAMES = 5;

    public String fingerprint(LogEvent e) {
        return sha256(buildSignature(e));
    }

    private String buildSignature(LogEvent e) {
        String stack = e.stackTrace();
        if (stack != null && !stack.isBlank()) {
            return normalizeStack(stack);
        }
        String exception = e.extractedFields().getOrDefault("exception", "");
        String svc = e.service() == null ? "" : e.service();
        String msgSkeleton = scrub(e.message() == null ? "" : e.message());
        return (exception + "|" + svc + "|" + msgSkeleton).strip();
    }

    private String normalizeStack(String stack) {
        String[] lines = stack.split("\\R");
        StringBuilder sb = new StringBuilder();
        int frames = 0;
        for (String line : lines) {
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                continue;
            }
            sb.append(scrub(trimmed)).append('\n');
            if (trimmed.startsWith("at ") && ++frames >= MAX_FRAMES) {
                break;
            }
        }
        return sb.toString().strip();
    }

    private String scrub(String s) {
        String out = s;
        out = LAMBDA_HASH.matcher(out).replaceAll("\\$\\$Lambda");
        out = OBJ_ID.matcher(out).replaceAll("@");
        out = HEX_ADDRS.matcher(out).replaceAll("0x");
        out = LINE_NUMBERS.matcher(out).replaceAll(":");
        out = DIGITS_RUN.matcher(out).replaceAll("#");
        out = WS.matcher(out).replaceAll(" ");
        return out.strip();
    }

    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.substring(0, 16);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
    }
}
