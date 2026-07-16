use std::path::PathBuf;
use std::pin::Pin;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::ros_installer_service_server::RosInstallerService;
use rqtll_api::rqtll::api::v1::{EnvInstallProgress, EnvInstallRequest, StepStatus, ConfigureEnvRequest};

use crate::utils::admin::run_apt_install_sudo;

#[derive(Debug, Default)]
pub struct MyROSInstallerService;

type ResponseStream = Pin<
    Box<dyn tokio_stream::Stream<Item = Result<EnvInstallProgress, Status>> + Send>,
>;

#[tonic::async_trait]
impl RosInstallerService for MyROSInstallerService {
    type InstallEnvironmentStream = ResponseStream;
    type SetupRepositoriesStream = ResponseStream;
    type ConfigureEnvironmentStream = ResponseStream;

    async fn install_environment(
        &self,
        _request: Request<EnvInstallRequest>,
    ) -> Result<Response<Self::InstallEnvironmentStream>, Status> {
        let (tx, rx) = mpsc::channel(128);

        let home_dir = std::path::PathBuf::from(
            std::env::var("HOME").map_err(|_| {
                Status::internal("No se pudo determinar el directorio HOME del usuario")
            })?,
        );

        tokio::spawn(async move {
            if let Err(e) = run_installation_workflow(home_dir, tx).await {
                eprintln!("Error en el flujo de instalación: {:?}", e);
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }

    async fn setup_repositories(
        &self,
        _request: Request<EnvInstallRequest>,
    ) -> Result<Response<Self::SetupRepositoriesStream>, Status> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            if let Err(e) = run_setup_repositories_workflow(tx).await {
                eprintln!("Error en la configuración de repositorios: {:?}", e);
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }

    async fn configure_environment(
        &self,
        request: Request<ConfigureEnvRequest>,
    ) -> Result<Response<Self::ConfigureEnvironmentStream>, Status> {
        let (tx, rx) = mpsc::channel(128);
        let req = request.into_inner();

        tokio::spawn(async move {
            if let Err(e) = run_configure_environment_workflow(req, tx).await {
                eprintln!("Error en la configuración del entorno: {:?}", e);
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ResponseStream
        ))
    }
}

async fn run_installation_workflow(
    home: PathBuf,
    tx: mpsc::Sender<Result<EnvInstallProgress, Status>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send_status(&tx, "CHECK_OS", "", StepStatus::Running, 5).await;
    let os_release = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    if !os_release.contains("Ubuntu")
        && !os_release.contains("Debian")
        && !os_release.contains("Mint")
    {
        send_status(
            &tx,
            "CHECK_OS_FAIL",
            "Distribución no soportada",
            StepStatus::Failed,
            5,
        )
        .await;
        return Ok(());
    }
    send_status(&tx, "CHECK_OS_SUCCESS", "", StepStatus::Running, 10).await;

    send_status(
        &tx,
        "INSTALL_DEPS",
        "Instalando dependencias base...",
        StepStatus::Running,
        15,
    )
    .await;

    let mut cmd = run_apt_install_sudo(&[
        "software-properties-common",
        "lsb-release",
        "gnupg",
        "curl",
    ])?;

    stream_command_output(&mut cmd, &tx, "INSTALL_DEPS", 20).await?;

    send_status(
        &tx,
        "DETECT_ROS_DISTRO",
        "Buscando versión compatible de ROS 2...",
        StepStatus::Running,
        40,
    )
    .await;
    let target_distros = vec!["jazzy", "humble", "rolling"];
    let mut selected_distro = "humble".to_string();

    for distro in target_distros {
        let output = Command::new("apt-cache")
            .args(&["search", &format!("ros-{}-desktop", distro)])
            .output()
            .await?;

        if !output.stdout.is_empty() {
            selected_distro = distro.to_string();
            break;
        }
    }
    send_status(
        &tx,
        "DETECT_ROS_DISTRO_SUCCESS",
        &format!("Seleccionada: {}", selected_distro),
        StepStatus::Running,
        45,
    )
    .await;

    let uros_ws = home.join("uros-ws");
    let uros_src = uros_ws.join("src");

    if uros_ws.exists() {
        let _ = tokio::fs::remove_dir_all(&uros_ws).await;
    }
    tokio::fs::create_dir_all(&uros_src).await?;

    send_status(
        &tx,
        "CLONE_MICROROS",
        "Clonando repositorio micro_ros_setup...",
        StepStatus::Running,
        60,
    )
    .await;

    let mut git_cmd = Command::new("git")
        .args(&[
            "clone",
            "-b",
            &selected_distro,
            "https://github.com/micro-ROS/micro_ros_setup.git",
            "micro_ros_setup",
        ])
        .current_dir(&uros_src)
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    stream_command_output(&mut git_cmd, &tx, "CLONE_MICROROS", 65).await?;

    send_status(
        &tx,
        "COLCON_BUILD",
        "Compilando entorno micro-ROS con colcon...",
        StepStatus::Running,
        80,
    )
    .await;

    let mut build_cmd = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "source /opt/ros/{}/setup.bash && colcon build",
                selected_distro
            ),
        ])
        .current_dir(&uros_ws)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    stream_command_output(&mut build_cmd, &tx, "COLCON_BUILD", 90).await?;

    send_status(
        &tx,
        "INSTALL_SUCCESS",
        "Entorno configurado correctamente.",
        StepStatus::Success,
        100,
    )
    .await;

    Ok(())
}

async fn stream_command_output(
    child: &mut tokio::process::Child,
    tx: &mpsc::Sender<Result<EnvInstallProgress, Status>>,
    step_id: &str,
    percentage: i32,
) -> Result<(), std::io::Error> {
    if let Some(stdout) = child.stdout.take() {
        let mut reader = BufReader::new(stdout).lines();
        while let Some(line) = reader.next_line().await? {
            let _ = tx
                .send(Ok(EnvInstallProgress {
                    step_id: step_id.to_string(),
                    log_line: line,
                    status: StepStatus::Running as i32,
                    progress_percentage: percentage,
                }))
                .await;
        }
    }

    let _ = child.wait().await;
    Ok(())
}

async fn send_status(
    tx: &mpsc::Sender<Result<EnvInstallProgress, Status>>,
    step_id: &str,
    log: &str,
    status: StepStatus,
    percentage: i32,
) {
    let _ = tx
        .send(Ok(EnvInstallProgress {
            step_id: step_id.to_string(),
            log_line: log.to_string(),
            status: status as i32,
            progress_percentage: percentage,
        }))
        .await;
}

async fn run_setup_repositories_workflow(
    tx: mpsc::Sender<Result<EnvInstallProgress, Status>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send_status(&tx, "SETUP_REPOS_CHECK", "Comprobando repositorios existentes...", StepStatus::Running, 0).await;

    let mut already_configured = false;
    for distro in &["bouncy", "crystal", "dashing", "eloquent", "foxy", "galactic", "humble", "iron", "jazzy", "kilted", "lyrical", "rolling"] {
        if let Ok(output) = Command::new("apt-cache")
            .args(&["search", &format!("ros-{}-desktop", distro)])
            .output()
            .await 
        {
            if !output.stdout.is_empty() {
                already_configured = true;
                break;
            }
        }
    }

    if already_configured {
        send_status(&tx, "SETUP_REPOS_SUCCESS", "Repositorios de ROS 2 ya configurados.", StepStatus::Success, 100).await;
        return Ok(());
    }

    send_status(&tx, "SETUP_REPOS_START", "Solicitando permisos de administrador...", StepStatus::Running, 5).await;

    let script = r#"
        exec 2>&1
        set -e
        echo "===PROGRESS:10:Actualizando índices de paquetes..."
        apt-get update

        echo "===PROGRESS:30:Instalando software-properties-common..."
        apt-get install -y software-properties-common

        echo "===PROGRESS:50:Añadiendo repositorio universe..."
        add-apt-repository -y universe

        echo "===PROGRESS:70:Instalando curl..."
        apt-get update && apt-get install -y curl

        echo "===PROGRESS:85:Descargando e instalando repositorio de ROS2..."
        ROS_APT_SOURCE_VERSION=$(curl -s https://api.github.com/repos/ros-infrastructure/ros-apt-source/releases/latest | grep -F "tag_name" | awk -F'"' '{print $4}')
        CODENAME=$(. /etc/os-release && echo ${UBUNTU_CODENAME:-${VERSION_CODENAME}})
        curl -L -o /tmp/ros2-apt-source.deb "https://github.com/ros-infrastructure/ros-apt-source/releases/download/${ROS_APT_SOURCE_VERSION}/ros2-apt-source_${ROS_APT_SOURCE_VERSION}.${CODENAME}_all.deb"
        dpkg -i /tmp/ros2-apt-source.deb

        echo "===PROGRESS:95:Instalando herramientas de desarrollo de ROS2..."
        apt-get update && apt-get install -y ros-dev-tools

        echo "===PROGRESS:100:Repositorios configurados con éxito."
    "#;

    let mut child = Command::new("pkexec")
        .args(&["bash", "-c", script])
        .stdout(std::process::Stdio::piped())
        .spawn()?;

    let mut current_percentage = 0;
    let current_step_id = "SETUP_REPOS".to_string();

    if let Some(stdout) = child.stdout.take() {
        let mut reader = BufReader::new(stdout).lines();
        while let Some(line) = reader.next_line().await? {
            if line.starts_with("===PROGRESS:") {
                let parts: Vec<&str> = line.splitn(3, ':').collect();
                if parts.len() == 3 {
                    if let Ok(pct) = parts[1].parse::<i32>() {
                        current_percentage = pct;
                    }
                    let msg = parts[2];
                    send_status(&tx, &current_step_id, msg, StepStatus::Running, current_percentage).await;
                    continue;
                }
            }
            
            let _ = tx.send(Ok(EnvInstallProgress {
                step_id: current_step_id.clone(),
                log_line: line,
                status: StepStatus::Running as i32,
                progress_percentage: current_percentage,
            })).await;
        }
    }

    let status = child.wait().await?;
    if status.success() {
        send_status(&tx, "SETUP_REPOS_SUCCESS", "Repositorios configurados con éxito.", StepStatus::Success, 100).await;
    } else {
        send_status(&tx, "SETUP_REPOS_FAILED", "Error al ejecutar la configuración con pkexec.", StepStatus::Failed, current_percentage).await;
    }

    Ok(())
}

async fn run_configure_environment_workflow(
    req: ConfigureEnvRequest,
    tx: mpsc::Sender<Result<EnvInstallProgress, Status>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    send_status(&tx, "CONFIGURE_START", "Iniciando configuración...", StepStatus::Running, 0).await;

    let home = std::path::PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/home/akey".to_string()));
    
    // 1. Determine user's login shell from /etc/passwd
    let username = std::env::var("USER").unwrap_or_else(|_| "akey".to_string());
    let default_shell = if let Ok(passwd) = std::fs::read_to_string("/etc/passwd") {
        let mut shell = "/bin/bash".to_string();
        for line in passwd.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 7 && parts[0] == username {
                shell = parts[6].to_string();
                break;
            }
        }
        shell
    } else {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string())
    };

    let shell_ext = if default_shell.contains("zsh") { "zsh" } else { "bash" };
    let config_path = home.join(format!(".{}rc", shell_ext));
    
    send_status(&tx, "CONFIGURE_SHELL", &format!("Detectada shell por defecto: {}. Configurando archivo: {:?}", default_shell, config_path), StepStatus::Running, 10).await;

    let content = if config_path.exists() {
        std::fs::read_to_string(&config_path).unwrap_or_default()
    } else {
        String::new()
    };

    let mut file_lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();

    // 2. Configure ROS 2 Source line
    if req.load_ros_shell {
        let source_line = format!("source /opt/ros/{}/setup.{}", req.ros_distro, shell_ext);
        send_status(&tx, "CONFIGURE_ROS_SHELL", "Configurando source de ROS 2 en la shell...", StepStatus::Running, 30).await;

        let mut updated_source = false;
        for line in file_lines.iter_mut() {
            if line.contains("source /opt/ros/") && (line.ends_with("/setup.bash") || line.ends_with("/setup.zsh") || line.ends_with("/setup.sh")) {
                *line = source_line.clone();
                updated_source = true;
                break;
            }
        }

        if !updated_source {
            file_lines.push(String::new());
            file_lines.push("# ROS 2 Environment".to_string());
            file_lines.push(source_line);
        }
    }

    // 3. Configure ROS_DOMAIN_ID, alias, and firewall
    if req.config_domain_id {
        send_status(&tx, "CONFIGURE_DOMAIN", &format!("Configurando ROS_DOMAIN_ID a {}...", req.domain_id), StepStatus::Running, 60).await;

        let port_base = 7400 + 250 * req.domain_id;
        let multicast_port = port_base;
        let data_multicast_port = port_base + 1;
        let unicast_port = port_base + 10;
        let data_unicast_port = port_base + 11;

        let export_line = format!("export ROS_DOMAIN_ID={}", req.domain_id);
        let alias_line = format!(
            "alias rqtll-ports=\"echo 'ROS_DOMAIN_ID actual: {}'; echo 'Puertos UFW autorizados para ROS 2 (RTPS):'; echo '  - Multicast Descubrimiento: {}/udp'; echo '  - Multicast Datos: {}/udp'; echo '  - Unicast Descubrimiento: {}/udp'; echo '  - Unicast Datos: {}/udp'\"",
            req.domain_id, multicast_port, data_multicast_port, unicast_port, data_unicast_port
        );

        let mut updated_export = false;
        let mut updated_alias = false;

        for line in file_lines.iter_mut() {
            if line.starts_with("export ROS_DOMAIN_ID=") {
                *line = export_line.clone();
                updated_export = true;
            } else if line.starts_with("alias rqtll-ports=") {
                *line = alias_line.clone();
                updated_alias = true;
            }
        }

        if !updated_export || !updated_alias {
            if !updated_export {
                file_lines.push(export_line.clone());
            }
            if !updated_alias {
                file_lines.push(alias_line.clone());
            }
        }

        send_status(&tx, "CONFIGURE_FIREWALL", "Abriendo puertos del firewall (UFW) para ROS_DOMAIN_ID...", StepStatus::Running, 80).await;
        for port in &[multicast_port, data_multicast_port, unicast_port, data_unicast_port] {
            let _ = Command::new("pkexec")
                .args(&["ufw", "allow", &format!("{}/udp", port)])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await;
        }
    }

    std::fs::write(&config_path, file_lines.join("\n") + "\n")?;

    send_status(&tx, "CONFIGURE_SUCCESS", "Configuración completada con éxito.", StepStatus::Success, 100).await;
    Ok(())
}
