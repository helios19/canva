package com.canva.queue.common.service;

import com.google.common.base.Strings;

/**
 * String utils class.
 */
public class StringUtils {

    public static String requireNonEmpty(String str, String message) {
        if (Strings.isNullOrEmpty(str))
            throw new IllegalArgumentException(message);
        return str;
    }

}
