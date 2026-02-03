package com.multiservicios.migrator.config;

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Component;

import com.zaxxer.hikari.HikariDataSource;

@Component
public class DataSourceRegistry {
	private final MigratorProperties properties;
	private final Map<String, DataSource> dataSourcesByName = new ConcurrentHashMap<>();

	public DataSourceRegistry(MigratorProperties properties) {
		this.properties = properties;
	}

	public DataSource getRequiredDataSource(String profileName) {
		return dataSourcesByName.computeIfAbsent(profileName, this::buildDataSource);
	}

	public Optional<ConnectionProfile> findProfile(String profileName) {
		return properties.getProfiles().stream()
				.filter(p -> Objects.equals(p.getName(), profileName))
				.findFirst();
	}

	public void testConnection(ConnectionProfile profile) throws Exception {
		HikariDataSource ds = null;
		try {
			ds = createDataSource(profile);
			try (Connection connection = ds.getConnection()) {
				int timeout = Math.max(1, profile.getValidationTimeoutSeconds());
				boolean ok = connection.isValid(timeout);
				if (!ok) {
					throw new IllegalStateException("La conexión respondió inválida (timeout=" + timeout + "s)");
				}
			}
		} finally {
			closeQuietly(ds);
		}
	}

	private DataSource buildDataSource(String profileName) {
		ConnectionProfile profile = findProfile(profileName)
				.orElseThrow(() -> new IllegalArgumentException("Perfil no encontrado: " + profileName));

		HikariDataSource ds = DataSourceBuilder.create()
				.type(HikariDataSource.class)
				.url(profile.getJdbcUrl())
				.username(profile.getUsername())
				.password(profile.getPassword())
				.driverClassName(profile.getDriverClassName())
				.build();

		ds.setPoolName("migrator-" + profileName);
		ds.setMaximumPoolSize(3);
		ds.setMinimumIdle(0);
		ds.setConnectionTimeout(Duration.ofSeconds(10).toMillis());
		ds.setValidationTimeout(Duration.ofSeconds(Math.max(1, profile.getValidationTimeoutSeconds())).toMillis());
		return ds;
	}

	public HikariDataSource createDataSource(ConnectionProfile profile) {
		HikariDataSource ds = DataSourceBuilder.create()
				.type(HikariDataSource.class)
				.url(profile.getJdbcUrl())
				.username(profile.getUsername())
				.password(profile.getPassword())
				.driverClassName(profile.getDriverClassName())
				.build();

		ds.setPoolName("migrator-dynamic");
		ds.setMaximumPoolSize(1);
		ds.setMinimumIdle(0);
		ds.setConnectionTimeout(Duration.ofSeconds(10).toMillis());
		ds.setValidationTimeout(Duration.ofSeconds(Math.max(1, profile.getValidationTimeoutSeconds())).toMillis());
		return ds;
	}

	public void closeQuietly(DataSource ds) {
		if (ds instanceof HikariDataSource hk) {
			try {
				hk.close();
			} catch (Exception ignored) {
			}
		}
	}

}
