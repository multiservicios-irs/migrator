package com.multiservicios.migrator.client;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.dto.CreateProductoResponse;

@Component
public class MultiServiciosProductoClient {
	private final RestClient restClient;

	public MultiServiciosProductoClient(MigratorProperties properties) {
		this.restClient = RestClient.builder()
				.baseUrl(properties.getDestino().getBaseUrl())
				.build();
	}

	public CreateProductoResponse createProducto(Object payload, boolean dryRun) {
		try {
			return restClient.post()
					.uri(uriBuilder -> uriBuilder.path("/api/productos").queryParam("dryRun", dryRun).build())
					.contentType(MediaType.APPLICATION_JSON)
					.body(payload)
					.retrieve()
					.body(CreateProductoResponse.class);
		} catch (RestClientResponseException ex) {
			String body = ex.getResponseBodyAsString();
			throw new IllegalStateException("HTTP " + ex.getStatusCode().value() + " al crear producto: " + (body == null || body.isBlank() ? ex.getMessage() : body));
		}
	}
}
