# Contribuyendo a rqtll-service

¡Gracias por contribuir al backend de RQTLL!

## Flujo de Trabajo en Rust

1. **Compilación**: Asegúrate de compilar y probar los cambios localmente:
   ```bash
   cargo build
   cargo test
   ```
2. **Formato**: Sigue el estándar de formato oficial de Rust. Ejecuta `cargo fmt` antes de enviar tus confirmaciones.
3. **Manejo de Errores**: Evita usar `unwrap()` o `expect()`. Utiliza la propagación de errores (`?`) y mapea los fallos del sistema a códigos de estado apropiados de gRPC (`tonic::Status`).
4. **Seguridad en Procesos**: Al lanzar comandos de consola asíncronos mediante `tokio::process::Command`, valida adecuadamente los argumentos para prevenir inyecciones y fugas de procesos en segundo plano.
5. **Pull Requests**: Explica con claridad la funcionalidad implementada, los servicios gRPC afectados y cómo probar los cambios.
