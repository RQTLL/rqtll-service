# rqtll-service

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/RQTLL/rqtll-components/blob/main/assets/branding/logo-main-light.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/RQTLL/rqtll-components/blob/main/assets/branding/logo-main-dark.svg">
  <img alt="RQTLL Logo" src="https://github.com/RQTLL/rqtll-components/blob/main/assets/branding/logo-main-color.svg" width="50px">
</picture>

Servidor backend asíncrono en Rust que sirve como motor de ejecución para RQTLL. Implementa los contratos definidos en `rqtll-api` utilizando **Tonic (gRPC)** y **Tokio**, permitiendo a la IDE interactuar de forma segura con ROS 2 y el sistema operativo.

## Table of Contents
- [rqtll-service](#rqtll-service)
  - [Table of Contents](#table-of-contents)
  - [Quickstart](#quickstart)
    - [Requisitos](#requisitos)
    - [Compilación e Instalación](#compilación-e-instalación)
    - [Ejecución](#ejecución)
  - [Arquitectura de Servicios](#arquitectura-de-servicios)
  - [Estructura del Repositorio](#estructura-del-repositorio)
  - [Mecanismos Clave](#mecanismos-clave)
    - [1. Gestión de Procesos e Introspección (SIGINT + SIGKILL)](#1-gestión-de-procesos-e-introspección-sigint--sigkill)
    - [2. Stream de Imágenes Optimizado (`image_bridge.py`)](#2-stream-de-imágenes-optimizado-image_bridgepy)
    - [3. Terminales Virtuales (PTY)](#3-terminales-virtuales-pty)
  - [Cómo contribuir](#cómo-contribuir)
  - [Security](#security)
  - [License](#license)
  - [Maintainers](#maintainers)

## Quickstart

### Requisitos

- `Rust` (stable, 1.70+ recomendado)
- `protoc` (Protocol Buffers Compiler)

### Compilación e Instalación

Para compilar el servicio en modo optimizado:
```bash
cargo build --release
```

### Ejecución

Ejecuta el binario generado (asegúrate de que ROS 2 esté en tus variables de entorno):
```bash
./target/release/rqtll_service
```

---

## Arquitectura de Servicios

El backend se organiza en servicios Tonic (`src/services/`) que implementan las siguientes capas:

| Servicio                  | Archivo                    | Descripción / Rol                                                                              |
| ------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------- |
| **WorkspaceService**      | `workspace.rs`             | Gestión de directorios de espacios de trabajo y detección de entornos colcon.                  |
| **BuildService**          | `build.rs`                 | Ejecución asíncrona de comandos `colcon build` / `cargo build` con streams de logs.            |
| **ExecutionService**      | `execution.rs`             | Lanzamiento no interactivo y monitoreo de nodos y lanzadores de ROS 2.                         |
| **DataStreamService**     | `data_stream.rs`           | Transmisión de tópicos de ROS (echo), estadísticas en tiempo real y flujo de vídeo comprimido. |
| **IntrospectionService**  | `introspection.rs`         | Consulta del grafo de ROS 2 (listado de nodos, tópicos y parámetros).                          |
| **TerminalService**       | `terminal.rs`              | Creación de consolas y shells interactivos mediante sesiones virtuales.                        |
| **InteractiveExecution**  | `interactive_execution.rs` | Ejecución de procesos con interacción de entrada/salida sobre PTYs.                            |
| **FileSystemService**     | `file_system.rs`           | Lectura, edición, listado de directorios y guardado de archivos de código.                     |
| **InstallerService**      | `installer.rs`             | Control del asistente de instalación automática de ROS 2 Lyrical/Jazzy.                        |
| **PackageManagerService** | `package.rs`               | Consulta e instalación de paquetes apt y bibliotecas de Python.                                |
| **SystemUtilsService**    | `system_utils.rs`          | Obtención de métricas de hardware (CPU/RAM) y bibliotecas de enlace dinámico.                  |
| **CloneWorkspaceService** | `clone.rs`                 | Clonación asíncrona y progresiva de repositorios git de workspaces.                            |

---

## Estructura del Repositorio

```text
./
├── external/                # Submódulos (rqtll-api)
├── src/                     # Código fuente de Rust
│   ├── services/            # Implementación de los servicios gRPC
│   │   ├── image_bridge.py  # Script auxiliar para procesar imágenes de ROS2 con cv2
│   │   ├── mod.rs           # Registro y despacho de servicios
│   │   └── [servicios].rs   # Lógica específica de cada servicio
│   ├── utils/               # Funciones de utilidad auxiliares
│   │   ├── admin.rs         # Comprobación de privilegios de administrador (root)
│   │   ├── apt.rs           # Envoltura de comandos apt-get asíncronos
│   │   └── fs.rs            # Auxiliares del sistema de archivos
│   └── main.rs              # Arranque del servidor gRPC y registro de Tonic
├── Cargo.toml               # Dependencias de Rust (Tonic, Tokio, Prost, Portable-PTY)
└── README.md
```

---

## Mecanismos Clave

### 1. Gestión de Procesos e Introspección (SIGINT + SIGKILL)
Para evitar que se queden procesos huérfanos (zombies) consumiendo la red de ROS al desconectarse la IDE (o cambiar de tema):
- El backend detecta la desconexión del stream mediante `tx.closed().await` en un hilo monitor.
- Envía inmediatamente una señal `SIGINT` (`kill -2`) al comando `ros2 topic echo/hz/bw`.
- Tras esperar **500 milisegundos**, ejecuta un **`SIGKILL` (`kill -9`)** preventivo a los procesos secundarios para garantizar su cierre inmediato del lado del sistema operativo.

### 2. Stream de Imágenes Optimizado (`image_bridge.py`)
Para transmitir flujos de vídeo de tópicos de cámara a altas tasas de fotogramas, `data_stream.rs` ejecuta `image_bridge.py` en segundo plano. Este puente de Python se suscribe al tópico de forma nativa en ROS y comprime la imagen a JPEG sobre stdout, evitando el lag y consumo excesivo de red.

### 3. Terminales Virtuales (PTY)
Implementado mediante la biblioteca `portable-pty` en `terminal.rs` e `interactive_execution.rs`, permitiendo una emulación completa de terminal interactiva con soporte para control de tamaño de ventana (`winsize`) de forma asíncrona.

---

## Cómo contribuir

- Lee [CONTRIBUTING.md](CONTRIBUTING.md) antes de enviar un Pull Request.
- Toda nueva característica debe mapearse primero en un archivo `.proto` de `rqtll-api`.
- Sigue las mejores prácticas de Rust: manejo explícito de errores (`Result`), código asíncrono no bloqueante con Tokio y tipado seguro.

## Security

Consulta [SECURITY.md](SECURITY.md) para conocer el procedimiento de reporte de vulnerabilidades.

## License

Este proyecto está bajo la licencia **MIT**. Consulta el archivo [LICENSE](LICENSE) para más detalles.

## Maintainers

* **adnKSharp** <adnksharp@gmail.com>
