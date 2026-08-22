package com.github.overz.shared.config;

public final class ConfigurationException extends RuntimeException {

  public ConfigurationException(final String message) {
    super(message);
  }

  public ConfigurationException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
