package com.github.overz.shared.config;

import lombok.extern.slf4j.Slf4j;

/**
 * A configuracao desta aplicacao, lida no config-server durante o boot e entregue como
 * record tipado.
 *
 * Faz uma coisa so: dizer quem e a aplicacao e devolver a config dela. O GET com retry
 * esta em ConfigServerHttp e a leitura do corpo em ConfigServerResponse -- os dois sao
 * estaticos e sem estado, entao esta classe nao precisa receber nada por construtor para
 * ser exercitada, e continua se montando sozinha a partir do ambiente.
 */
@Slf4j
public final class ConfigServerClient {

  private final ConfigServerSettings settings = ConfigServerSettings.shared();

  // Um processo fala com um config-server so, e a configuracao e lida uma vez no boot.
  // Guardar a instancia evita que cada ponto que precise do nome da aplicacao ou de uma
  // secao da config monte um cliente novo -- e nao e static: continua sendo um objeto,
  // criado sob demanda a partir do ambiente.
  private static final class Holder {
    private static final ConfigServerClient INSTANCE = new ConfigServerClient();
  }

  public static ConfigServerClient shared() {
    return Holder.INSTANCE;
  }

  public String application() {
    return settings.application();
  }

  public <T> T fetch(final Class<T> type) {
    // A URL ja diz o profile quando ha um; sem profile quem resolve e o config-server, e
    // um campo perfil='' aqui so enganaria quem le o log.
    log.info("carregando configuracao, aplicacao='{}', url='{}'",
      settings.application(), settings.uri());

    final var config = ConfigServerResponse.parse(settings, ConfigServerHttp.get(settings), type);

    log.info("configuracao carregada e validada, aplicacao='{}', tipo='{}'",
      settings.application(), type.getSimpleName());

    return config;
  }
}
