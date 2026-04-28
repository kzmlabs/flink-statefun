// SPDX-License-Identifier: Apache-2.0
// Copyright 2014 The Apache Software Foundation
package org.apache.flink.statefun.flink.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import javax.annotation.Nullable;

public final class ResourceLocator {
  private ResourceLocator() {}

  public static Iterable<URL> findResources(String namedResource) {
    if (namedResource.startsWith("classpath:")) {
      return findNamedResources(namedResource);
    } else {
      URL u = findNamedResource(namedResource);
      return Collections.singletonList(u);
    }
  }

  /** Locates a resource with a given name in the classpath or url path. */
  public static Iterable<URL> findNamedResources(String name) {
    URI nameUri = URI.create(name);
    if (!isClasspath(nameUri)) {
      throw new IllegalArgumentException(
          "unsupported or missing schema <"
              + nameUri.getScheme()
              + "> classpath: schema is supported.");
    }
    return urlClassPathResource(nameUri);
  }

  public static URL findNamedResource(final String name) {
    URI nameUri = URI.create(name);
    if (isClasspath(nameUri)) {
      Iterable<URL> resources = urlClassPathResource(nameUri);
      return firstElementOrNull(resources);
    }
    try {
      if (nameUri.isAbsolute()) {
        return nameUri.toURL();
      }
      // this URI is missing a schema (non absolute), therefore we assume that is a file.
      return new URL("file:" + name);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static boolean isClasspath(URI nameUri) {
    @Nullable String scheme = nameUri.getScheme();
    if (scheme == null) {
      return false;
    }
    return scheme.equalsIgnoreCase("classpath");
  }

  private static Iterable<URL> urlClassPathResource(URI uri) {
    ClassLoader cl =
        firstNonNull(
            Thread.currentThread().getContextClassLoader(), ResourceLocator.class.getClassLoader());
    try {
      Enumeration<URL> enumeration = cl.getResources(uri.getSchemeSpecificPart());
      return asIterable(enumeration);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static URL firstElementOrNull(Iterable<URL> urls) {
    for (URL url : urls) {
      return url;
    }
    return null;
  }

  private static <T> Iterable<T> asIterable(Enumeration<T> enumeration) {
    return () ->
        new Iterator<T>() {
          @Override
          public boolean hasNext() {
            return enumeration.hasMoreElements();
          }

          @Override
          public T next() {
            return enumeration.nextElement();
          }
        };
  }

  private static <T> T firstNonNull(T a, T b) {
    return (a != null) ? a : b;
  }
}
