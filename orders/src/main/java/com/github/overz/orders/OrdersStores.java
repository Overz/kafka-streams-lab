package com.github.overz.orders;

import lombok.Builder;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Nomes dos state stores, vindos da configuracao pelo mesmo motivo dos topicos: mudar o
 * nome de um store e uma migracao de estado, e ela precisa estar visivel num .yaml
 * versionado, nao escondida numa constante Java.
 */
@With
@Builder
@Jacksonized
public record OrdersStores(
    String orderState
) {
}
