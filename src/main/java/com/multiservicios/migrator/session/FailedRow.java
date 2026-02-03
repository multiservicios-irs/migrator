package com.multiservicios.migrator.session;

import java.util.Map;

public record FailedRow(Map<String, Object> row, String error, int attempts) {
}
