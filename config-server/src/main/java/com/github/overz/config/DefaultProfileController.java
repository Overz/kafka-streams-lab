package com.github.overz.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.server.environment.EnvironmentController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * Consulta por namespace + aplicacao, sem o segmento de profile:
 * {@code GET /lab/orders} em vez de {@code GET /lab/orders/local}.
 *
 * <p>O Spring Cloud Config nao serve isso de fabrica. O {@code EnvironmentController} so
 * declara {@code /{name}/{profiles}} e {@code /{name}/{profiles}/{label}} -- dois
 * segmentos no minimo depois do prefixo -- e
 * {@code spring.cloud.config.server.default-profile} nao alcanca a camada MVC: quem o le
 * e a configuracao de encryption e os repositorios da AWS.
 *
 * <p>Delega ao proprio {@code EnvironmentController} em vez de falar direto com o
 * {@code EnvironmentRepository}: overrides, encryption e tratamento de erro continuam
 * sendo os do Spring, e nada disso e reimplementado aqui.
 *
 * <p>A forma longa continua valendo -- {@code /lab/orders/prod} escolhe outro profile sem
 * passar por esta classe.
 */
@RestController
@RequiredArgsConstructor
public class DefaultProfileController {

  private final EnvironmentController delegate;

  // Campo final com @Value so funciona porque lombok.config copia a anotacao para o
  // parametro do construtor gerado (lombok.copyableAnnotations). Sem isso ela ficaria
  // parada no campo, onde o Spring nao a le, e o profile chegaria nulo.
  @Value("${lab.config.default-profile:local}")
  private final String defaultProfile;

  // Mesmo placeholder que o EnvironmentController usa no @RequestMapping da classe dele,
  // entao o atalho acompanha o prefixo sem precisar saber como o Spring o aplica.
  @GetMapping("${spring.cloud.config.server.prefix:}/{application}")
  public Environment byApplication(@PathVariable final String application) {
    return delegate.defaultLabel(application, defaultProfile);
  }
}
