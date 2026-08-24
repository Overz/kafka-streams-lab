package com.github.overz.shared.time;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Clock;
import java.time.Instant;

/**
 * O relogio do lab, em UTC, num lugar so. Todo carimbo de tempo dos eventos sai daqui:
 * duas etapas do pipeline lendo relogios diferentes produziriam eventos que nao ordenam
 * entre si.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class Clocks {

  private static final Clock SYSTEM = Clock.systemUTC();

  public static Clock system() {
    return SYSTEM;
  }

  public static Instant now() {
    return SYSTEM.instant();
  }
}
