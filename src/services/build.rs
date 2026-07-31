use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use tokio::io::{AsyncBufReadExt, BufReader};

use rqtll_api::rqtll::api::v1::build_service_server::BuildService;
use rqtll_api::rqtll::api::v1::{
    BuildRequest, BuildEvent, CleanRequest, LoadRequest, Status as ApiStatus,
    LogEntry, LogLevel,
};

use crate::services::workspace::{ACTIVE_WORKSPACE, scan_packages_in_workspace};

pub struct MyBuildService;

impl Default for MyBuildService {
    fn default() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl BuildService for MyBuildService {
    type BuildWorkspaceStream = Pin<Box<dyn Stream<Item = Result<BuildEvent, Status>> + Send>>;

    async fn build_workspace(
        &self,
        req: Request<BuildRequest>,
    ) -> Result<Response<Self::BuildWorkspaceStream>, Status> {
        let req = req.into_inner();
        let ws_path = if !req.workspace_path.is_empty() {
            req.workspace_path.clone()
        } else {
            if let Ok(lock) = ACTIVE_WORKSPACE.lock() {
                lock.clone().unwrap_or_else(|| "/home/akey/Proyectos/rqtll".to_string())
            } else {
                "/home/akey/Proyectos/rqtll".to_string()
            }
        };

        let mut args = vec!["build".to_string()];
        if req.symlink_install {
            args.push("--symlink-install".to_string());
        }
        for carg in req.colcon_args {
            args.push(carg);
        }

        let mut child = tokio::process::Command::new("colcon")
            .args(&args)
            .current_dir(&ws_path)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to run colcon build: {e}")))?;

        let stdout = child.stdout.take().ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| Status::internal("Failed to open stderr"))?;

        let (tx, rx) = mpsc::channel(128);

        let tx_out = tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let event = BuildEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::build_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Info as i32,
                        source: "colcon".to_string(),
                        message: line,
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
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let event = BuildEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::build_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Warn as i32,
                        source: "colcon".to_string(),
                        message: line,
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

            let mut details = HashMap::new();
            let mut status_msg = "Build failed".to_string();

            if success {
                status_msg = "Build completed successfully".to_string();

                let setup_path = PathBuf::from(&ws_path).join("install/setup.bash");
                let has_setup = setup_path.exists();
                let pkgs = scan_packages_in_workspace(&ws_path);

                let mut nodes = Vec::new();
                let mut launchers = Vec::new();

                let distro = crate::utils::apt::get_ros_distro().await;
                let sys_setup = format!("/opt/ros/{}/setup.bash", distro);
                let mut source_parts = Vec::new();
                if let Some(domain_id) = crate::utils::apt::get_configured_domain_id() {
                    source_parts.push(format!("export ROS_DOMAIN_ID={}", domain_id));
                }
                if std::path::Path::new(&sys_setup).exists() {
                    source_parts.push(format!("source {}", sys_setup));
                }
                if has_setup {
                    source_parts.push(format!("source {}", setup_path.to_string_lossy()));
                }
                let source_prefix = if source_parts.is_empty() {
                    "".to_string()
                } else {
                    format!("{} && ", source_parts.join(" && "))
                };

                for pkg in pkgs {
                    // 1. Get executables (nodes) for this workspace package
                    if has_setup {
                        let cmd_str = format!("{}ros2 pkg executables {}", source_prefix, pkg.name);
                        let out = tokio::process::Command::new("bash")
                            .args(&["-c", &cmd_str])
                            .output()
                            .await;
                        if let Ok(output) = out {
                            let out_str = String::from_utf8_lossy(&output.stdout);
                            for line in out_str.lines() {
                                let parts: Vec<&str> = line.split_whitespace().collect();
                                if parts.len() >= 2 {
                                    nodes.push(format!("{}/{}", parts[0], parts[1]));
                                }
                            }
                        }
                    }

                    // 2. Scan install/<pkg>/share/<pkg> for launchers
                    let pkg_share = PathBuf::from(&ws_path).join("install").join(&pkg.name).join("share").join(&pkg.name);
                    if pkg_share.exists() {
                        fn find_pkg_launchers(dir: &PathBuf, pkg_name: &str, list: &mut Vec<String>) {
                            if let Ok(entries) = std::fs::read_dir(dir) {
                                for entry in entries.filter_map(Result::ok) {
                                    let path = entry.path();
                                    if path.is_dir() {
                                        find_pkg_launchers(&path, pkg_name, list);
                                    } else if path.is_file() {
                                        if let Some(ext) = path.extension() {
                                            if ext == "py" || ext == "xml" || ext == "yaml" {
                                                if let Some(stem) = path.file_stem() {
                                                    let stem_str = stem.to_string_lossy().to_string();
                                                    if stem_str.contains(".launch") {
                                                        let full_launch_name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
                                                        list.push(format!("{}/{}", pkg_name, full_launch_name));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        find_pkg_launchers(&pkg_share, &pkg.name, &mut launchers);
                    }
                }

                if let Ok(nodes_json) = serde_json::to_string(&nodes) {
                    details.insert("nodes".to_string(), nodes_json);
                }
                if let Ok(launchers_json) = serde_json::to_string(&launchers) {
                    details.insert("launchers".to_string(), launchers_json);
                }
            }

            let final_event = BuildEvent {
                ev: Some(rqtll_api::rqtll::api::v1::build_event::Ev::Status(ApiStatus {
                    ok: success,
                    code: if success { 0 } else { 2 },
                    message: status_msg,
                    details,
                })),
            };

            let _ = tx.send(Ok(final_event)).await;
        });

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::BuildWorkspaceStream))
    }

    async fn clean_workspace(
        &self,
        req: Request<CleanRequest>,
    ) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let ws_path = if let Ok(lock) = ACTIVE_WORKSPACE.lock() {
            lock.clone().unwrap_or_else(|| "/home/akey/Proyectos/rqtll".to_string())
        } else {
            "/home/akey/Proyectos/rqtll".to_string()
        };

        let ws_buf = PathBuf::from(ws_path);
        let mut removed = Vec::new();

        if req.clean_build {
            let p = ws_buf.join("build");
            if p.exists() {
                let _ = std::fs::remove_dir_all(&p);
                removed.push("build");
            }
        }
        if req.clean_install {
            let p = ws_buf.join("install");
            if p.exists() {
                let _ = std::fs::remove_dir_all(&p);
                removed.push("install");
            }
        }
        if req.clean_log {
            let p = ws_buf.join("log");
            if p.exists() {
                let _ = std::fs::remove_dir_all(&p);
                removed.push("log");
            }
        }

        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: format!("Workspace cleaned folders: {:?}", removed),
            details: HashMap::new(),
        }))
    }

    async fn load_overlay(
        &self,
        _req: Request<LoadRequest>,
    ) -> Result<Response<ApiStatus>, Status> {
        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Overlay loaded".to_string(),
            details: HashMap::new(),
        }))
    }
}
