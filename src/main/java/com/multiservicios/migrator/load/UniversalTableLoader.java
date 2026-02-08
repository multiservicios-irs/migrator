package com.multiservicios.migrator.load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.multiservicios.migrator.config.MigratorProperties;
import com.multiservicios.migrator.model.ColumnMeta;

@Component
public class UniversalTableLoader {
	private final UniversalTypeAdapter typeAdapter;
	private final MigratorProperties properties;

	public UniversalTableLoader(UniversalTypeAdapter typeAdapter, MigratorProperties properties) {
		this.typeAdapter = typeAdapter;
		this.properties = properties;
	}

	public record LoadResult(boolean success, String message) {
	}

	public LoadResult insertRow(DataSource destino, String schema, String table, Map<String, Object> values,
			Set<String> allowedColumnsLower, boolean dryRun) {
		return insertRow(destino, schema, table, values, allowedColumnsLower, null, dryRun);
	}

	public LoadResult insertRow(DataSource destino, String schema, String table, Map<String, Object> values,
			Set<String> allowedColumnsLower, Map<String, ColumnMeta> columnMetaByLower, boolean dryRun) {
		if (destino == null) {
			return new LoadResult(false, "Destino es null");
		}
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		if (safeTable.isBlank()) {
			return new LoadResult(false, "Tabla destino es requerida");
		}
		if (values == null || values.isEmpty()) {
			return new LoadResult(false, "No hay columnas mapeadas para insertar");
		}

		// Filtrar por columnas permitidas (defensa fuerte)
		List<String> columns = new ArrayList<>();
		List<Object> params = new ArrayList<>();
		for (var e : values.entrySet()) {
			String col = e.getKey() == null ? "" : e.getKey().trim();
			if (col.isEmpty()) {
				continue;
			}
			if (allowedColumnsLower != null && !allowedColumnsLower.isEmpty()) {
				if (!allowedColumnsLower.contains(col.toLowerCase())) {
					continue;
				}
			}

			// Si el destino marca la columna como autogenerada, NO la insertamos.
			// Ej: id SERIAL/IDENTITY/AUTO_INCREMENT.
			if (columnMetaByLower != null && !columnMetaByLower.isEmpty()) {
				ColumnMeta meta = columnMetaByLower.get(col.toLowerCase());
				if (meta != null && (meta.autoIncrement() || meta.generated())) {
					continue;
				}
			}
			Object rawValue = e.getValue();
			Object converted = rawValue;
			if (columnMetaByLower != null && !columnMetaByLower.isEmpty()) {
				ColumnMeta meta = columnMetaByLower.get(col.toLowerCase());
				try {
					converted = typeAdapter.convert(meta, rawValue);
				} catch (Exception ex) {
					return new LoadResult(false, "Columna '" + col + "': " + ex.getMessage());
				}
			}
			columns.add(col);
			params.add(converted);
		}
		if (columns.isEmpty()) {
			return new LoadResult(false,
					"No hay columnas válidas para insertar (revisá mapeos vs columnas reales del destino)");
		}

		try (Connection c = destino.getConnection()) {
			String sql = buildInsertSql(c, safeSchema, safeTable, columns);
			if (dryRun) {
				return new LoadResult(true, "DRY RUN: " + sql);
			}
			try (PreparedStatement ps = c.prepareStatement(sql)) {
			for (int i = 0; i < params.size(); i++) {
				ps.setObject(i + 1, params.get(i));
			}
			int updated = ps.executeUpdate();
			if (updated == 1) {
				return new LoadResult(true, "Insert OK en " + safeSchema + "." + safeTable + " (" + updated + ")");
			}
			// En PostgreSQL con ON CONFLICT DO NOTHING, updated puede ser 0 si hubo duplicado.
			if (updated == 0 && shouldSkipDuplicates()) {
				return new LoadResult(true, "SKIP duplicado (ya existe en " + safeSchema + "." + safeTable + ")");
			}
			return new LoadResult(false, "No se insertó la fila (updated=" + updated + ")");
			}
		} catch (SQLException ex) {
			if (shouldSkipDuplicates() && isUniqueViolation(ex)) {
				return new LoadResult(true, "SKIP duplicado (unique_violation)");
			}
			return new LoadResult(false, ex.getClass().getSimpleName() + ": " + ex.getMessage());
		} catch (Exception ex) {
			return new LoadResult(false, ex.getClass().getSimpleName() + ": " + ex.getMessage());
		}
	}

	public List<ColumnMeta> describeColumns(DataSource destino, String schema, String table) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		List<ColumnMeta> cols = new ArrayList<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getColumns(null, safeSchema, safeTable, null)) {
				while (rs.next()) {
					String name = rs.getString("COLUMN_NAME");
					int dataType = rs.getInt("DATA_TYPE");
					String typeName = rs.getString("TYPE_NAME");
					Integer columnSize = getIntOrNull(rs, "COLUMN_SIZE");
					Integer decimalDigits = getIntOrNull(rs, "DECIMAL_DIGITS");
					Integer nullableFlag = getIntOrNull(rs, "NULLABLE");
					boolean nullable = nullableFlag == null || nullableFlag != java.sql.DatabaseMetaData.columnNoNulls;
					String def = rs.getString("COLUMN_DEF");
					boolean autoInc = "YES".equalsIgnoreCase(getStringQuietly(rs, "IS_AUTOINCREMENT"));
					boolean generated = "YES".equalsIgnoreCase(getStringQuietly(rs, "IS_GENERATEDCOLUMN"));
					cols.add(new ColumnMeta(name, dataType, typeName, columnSize, decimalDigits, nullable, def, autoInc, generated));
				}
			}
		}
		return cols;
	}

	public Map<String, ColumnMeta> loadColumnMetaByLower(DataSource destino, String schema, String table) throws SQLException {
		List<ColumnMeta> list = describeColumns(destino, schema, table);
		Map<String, ColumnMeta> map = new LinkedHashMap<>();
		for (ColumnMeta c : list) {
			if (c == null || c.name() == null || c.name().isBlank()) {
				continue;
			}
			map.put(c.nameLower(), c);
		}
		return map;
	}

	public Set<String> loadAllowedColumnsLower(DataSource destino, String schema, String table) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		Set<String> cols = new LinkedHashSet<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getColumns(null, safeSchema, safeTable, null)) {
				while (rs.next()) {
					String col = rs.getString("COLUMN_NAME");
					if (col != null && !col.isBlank()) {
						cols.add(col.toLowerCase());
					}
				}
			}
		}
		return cols;
	}

	public List<String> listTables(DataSource destino, String schema) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		List<String> tables = new ArrayList<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getTables(null, safeSchema, "%", new String[] { "TABLE" })) {
				while (rs.next()) {
					String t = rs.getString("TABLE_NAME");
					if (t != null && !t.isBlank()) {
						tables.add(t);
					}
				}
			}
		}
		return tables;
	}

	public List<String> listColumns(DataSource destino, String schema, String table) throws SQLException {
		String safeSchema = (schema == null || schema.isBlank()) ? "public" : schema.trim();
		String safeTable = table == null ? "" : table.trim();
		List<String> cols = new ArrayList<>();
		try (Connection c = destino.getConnection()) {
			var md = c.getMetaData();
			try (ResultSet rs = md.getColumns(null, safeSchema, safeTable, null)) {
				while (rs.next()) {
					String col = rs.getString("COLUMN_NAME");
					if (col != null && !col.isBlank()) {
						cols.add(col);
					}
				}
			}
		}
		return cols;
	}

	private String buildInsertSql(Connection c, String schema, String table, List<String> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append("insert into ");
		sb.append(quoteIdent(schema)).append('.').append(quoteIdent(table));
		sb.append(" (");
		for (int i = 0; i < columns.size(); i++) {
			if (i > 0) sb.append(", ");
			sb.append(quoteIdent(columns.get(i)));
		}
		sb.append(") values (");
		for (int i = 0; i < columns.size(); i++) {
			if (i > 0) sb.append(", ");
			sb.append('?');
		}
		sb.append(')');

		if (shouldSkipDuplicates() && isPostgres(c)) {
			sb.append(" on conflict do nothing");
		}
		return sb.toString();
	}

	private boolean shouldSkipDuplicates() {
		try {
			if (properties == null || properties.getPolicies() == null || properties.getPolicies().getDuplicatePolicy() == null) {
				return true;
			}
			return properties.getPolicies().getDuplicatePolicy() == MigratorProperties.DuplicatePolicy.SKIP;
		} catch (Exception ex) {
			return true;
		}
	}

	private static boolean isPostgres(Connection c) {
		if (c == null) {
			return false;
		}
		try {
			String name = c.getMetaData().getDatabaseProductName();
			return name != null && name.toLowerCase().contains("postgres");
		} catch (Exception ex) {
			return false;
		}
	}

	private static boolean isUniqueViolation(SQLException ex) {
		// PostgreSQL: 23505 = unique_violation
		return ex != null && "23505".equals(ex.getSQLState());
	}

	private static String quoteIdent(String ident) {
		String s = ident == null ? "" : ident.trim();
		// double quote escaping
		s = s.replace("\"", "\"\"");
		return "\"" + s + "\"";
	}

	private static Integer getIntOrNull(ResultSet rs, String col) throws SQLException {
		int v = rs.getInt(col);
		return rs.wasNull() ? null : v;
	}

	private static String getStringQuietly(ResultSet rs, String col) {
		try {
			return rs.getString(col);
		} catch (Exception ex) {
			return null;
		}
	}
}
