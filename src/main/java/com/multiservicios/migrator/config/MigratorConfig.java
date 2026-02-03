package com.multiservicios.migrator.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MigratorProperties.class)
public class MigratorConfig {
}
