# migrator

Aplicación Spring Boot para ejecutar procesos de migración de datos.

## Requisitos

- Java 21
- Maven (incluye wrapper: `mvnw` / `mvnw.cmd`)

## Ejecutar en local

En Windows (PowerShell/CMD):

```bash
./mvnw.cmd spring-boot:run
```

En Linux/macOS:

```bash
./mvnw spring-boot:run
```

## Tests

```bash
./mvnw.cmd test
```

## Notas

- Configuración en `src/main/resources/application.properties`.
- Por defecto, el directorio `target/` está ignorado por Git.
