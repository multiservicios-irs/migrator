package com.multiservicios.migrator.session.requests;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;

public class RunAllRequest {
	@NotNull
	private UUID sessionId;

	private boolean dryRun;

	private Integer maxItems;

	public Integer getMaxItems() {
		return maxItems;
	}
	
	public UUID getSessionId() {
		return sessionId;
	}

	public void setSessionId(UUID sessionId) {
		this.sessionId = sessionId;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public void setMaxItems(Integer maxItems) {
		this.maxItems = maxItems;
	}
}
