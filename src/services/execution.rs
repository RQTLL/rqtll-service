use std::collections::HashMap;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::execution_service_server::ExecutionService;
use rqtll_api::rqtll::api::v1::{
    Empty, ExecEvent, LaunchListResponse, LogEntry, LogLevel, RunRequest, Status as ApiStatus,
    StopRequest,
};

use crate::services::workspace::ACTIVE_WORKSPACE;

#[derive(Debug, Default)]
pub struct MyExecutionService;

#[tonic::async_trait]
impl ExecutionService for MyExecutionService {
    type RunStream = ReceiverStream<Result<ExecEvent, Status>>;

    async fn run(&self, req: Request<RunRequest>) -> Result<Response<Self::RunStream>, Status> {
        let req = req.into_inner();
        let ws_path = {
            if let Ok(lock) = ACTIVE_WORKSPACE.lock() {
                lock.clone()
                    .unwrap_or_else(|| "/home/akey/Proyectos/rqtll".to_string())
            } else {
                "/home/akey/Proyectos/rqtll".to_string()
            }
        };

        crate::utils::apt::load_ros_environment(&ws_path).await;

        let distro = crate::utils::apt::get_ros_distro().await;
        let sys_setup = format!("/opt/ros/{}/setup.bash", distro);
        let mut source_parts = Vec::new();
        if let Some(domain_id) = crate::utils::apt::get_configured_domain_id() {
            source_parts.push(format!("export ROS_DOMAIN_ID={}", domain_id));
        }
        if std::path::Path::new(&sys_setup).exists() {
            source_parts.push(format!("source {}", sys_setup));
        }
        let setup_path = PathBuf::from(&ws_path).join("install/setup.bash");
        if setup_path.exists() {
            source_parts.push(format!("source {}", setup_path.to_string_lossy()));
        }

        let run_cmd = if req.use_launch {
            format!("ros2 launch {} {}", req.package, req.launch_file)
        } else {
            format!("ros2 run {} {}", req.package, req.executable)
        };

        let cmd_str = if source_parts.is_empty() {
            run_cmd
        } else {
            format!("{} && {}", source_parts.join(" && "), run_cmd)
        };

        let mut child = tokio::process::Command::new("bash")
            .args(&["-c", &cmd_str])
            .env("RCUTILS_COLORIZED_OUTPUT", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to run node/launcher: {e}")))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Status::internal("Failed to open stderr"))?;

        let (tx, rx) = mpsc::channel(64);
        let pid = child.id();

        // Monitor stream cancellation to terminate the process cleanly
        let tx_monitor = tx.clone();
        tokio::spawn(async move {
            tx_monitor.closed().await;
            if let Some(p) = pid {
                let _ = std::process::Command::new("kill")
                    .args(&["-2", &p.to_string()])
                    .status();
            }
        });

        let tx_out = tx.clone();
        tokio::spawn(async move {
            let mut reader = stdout;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = ExecEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::exec_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Info as i32,
                        source: "execution_service".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_out.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        let tx_err = tx.clone();
        tokio::spawn(async move {
            let mut reader = stderr;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = ExecEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::exec_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Warn as i32,
                        source: "execution_service".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_err.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let exit_status = child.wait().await;
            let success = match exit_status {
                Ok(status) => status.success(),
                Err(_) => false,
            };
            let final_event = ExecEvent {
                ev: Some(rqtll_api::rqtll::api::v1::exec_event::Ev::Status(
                    ApiStatus {
                        ok: success,
                        code: if success { 0 } else { 2 },
                        message: if success {
                            "Process exited successfully"
                        } else {
                            "Process stopped"
                        }
                        .to_string(),
                        details: HashMap::new(),
                    },
                )),
            };
            let _ = tx.send(Ok(final_event)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stop(&self, _req: Request<StopRequest>) -> Result<Response<ApiStatus>, Status> {
        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Stopped".to_string(),
            details: HashMap::new(),
        }))
    }

    async fn list_launch_files(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<LaunchListResponse>, Status> {
        Ok(Response::new(LaunchListResponse {
            launch_files: vec![],
            status: Some(ApiStatus {
                ok: true,
                code: 0,
                message: "Success".to_string(),
                details: HashMap::new(),
            }),
        }))
    }
}
