# Security Policy - rqtll-service

Si detectas un problema de seguridad o vulnerabilidad en el backend de `rqtll-service`, por favor repórtalo siguiendo este procedimiento.

## Reporte responsable

1. Envía un correo con los detalles técnicos a:
   - **adnKSharp** <adnksharp@gmail.com>

2. Incluye en tu correo:
   - Pasos detallados para reproducir el exploit.
   - Entorno y versiones afectadas.

3. Por favor, no reveles detalles del fallo en foros o redes públicas hasta que hayamos liberado un parche corrector.

## Respuesta y Tiempos

- Confirmaremos recepción del mensaje en un plazo de **36 horas**.
- Evaluaremos y distribuiremos un parche de seguridad en un plazo máximo de **7 días hábiles**.

## Políticas de seguridad específicas para rqtll-service

- **Inyección de Comandos Shell**: Los servicios que ejecutan comandos en terminal (como `BuildService`, `ExecutionService` y `InteractiveExecution`) deben evitar el uso de intérpretes de comandos libres (`sh -c`) siempre que sea posible. Se deben pasar los argumentos como vectores independientes (`args()`) para evitar inyecciones.
- **Privilegios de Daemon**: El daemon de RQTLL se ejecuta en segundo plano. Nunca debe ejecutarse con privilegios de `root` (superusuario) a menos que sea estrictamente necesario para operaciones del sistema específicas, las cuales deben estar aisladas y validadas mediante autenticación.
- **Fuga de Subprocesos**: El monitor de procesos de introspección debe asegurar la muerte en cascada de los procesos secundarios (`kill -9`) tras la desconexión del cliente, para evitar ataques de denegación de servicio por agotamiento de descriptores de procesos en el sistema anfitrión.
