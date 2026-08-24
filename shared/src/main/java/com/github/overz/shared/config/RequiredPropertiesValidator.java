package com.github.overz.shared.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Percorre a arvore de records procurando componente nulo. Tudo e obrigatorio por padrao;
 * o que pode faltar declara-se como Optional<T>, e o proprio tipo passa a documentar isso.
 * <p>
 * Existe para que uma chave esquecida no .yaml apareca como uma mensagem nomeando a chave,
 * no boot, e nao como NullPointerException no meio do pipeline horas depois.
 */
final class RequiredPropertiesValidator {

  private RequiredPropertiesValidator() {
  }

  static void validate(final Object config, final String application) {
    final var missing = new ArrayList<String>();
    collect(config, List.of(), missing);

    if (!missing.isEmpty()) {
      throw new ConfigurationException(
        "configuracao incompleta para a aplicacao '%s': %d chave(s) obrigatoria(s) ausente(s) -> %s"
          .formatted(application, missing.size(), String.join(", ", missing)));
    }
  }

  private static void collect(final Object node, final List<String> path, final List<String> missing) {
    if (node == null || !node.getClass().isRecord()) {
      return;
    }

    for (final var component : node.getClass().getRecordComponents()) {
      final var childPath = new ArrayList<>(path);
      childPath.add(component.getName());

      final Object value;
      try {
        value = component.getAccessor().invoke(node);
      } catch (final ReflectiveOperationException e) {
        throw new ConfigurationException(
          "nao foi possivel ler a propriedade '%s'".formatted(ConfigTree.toPropertyKey(childPath)), e);
      }

      // Optional declara explicitamente que a ausencia e aceitavel; nao entra na conta.
      if (Optional.class.equals(component.getType())) {
        continue;
      }

      if (value == null) {
        missing.add(ConfigTree.toPropertyKey(childPath));
        continue;
      }

      collect(value, childPath, missing);
    }
  }
}
