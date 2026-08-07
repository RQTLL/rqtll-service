use rqtll_api::rqtll::api::v1::system_utils_server::SystemUtils;
use rqtll_api::rqtll::api::v1::{
    AvailableLibrariesResponse, CommandOutput, CommandRequest, Empty, RemoteExecRequest, SshConfig,
    SshSession, Status as ApiStatus,
};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use tonic::{Request, Response, Status};

fn scan_python_libraries() -> Vec<String> {
    let mut libs = Vec::new();
    let home = std::env::var("HOME").unwrap_or_default();

    let search_roots = vec![
        PathBuf::from("/usr/lib"),
        PathBuf::from("/usr/local/lib"),
        PathBuf::from(format!("{}/.local/lib", home)),
    ];

    for root in search_roots {
        if let Ok(entries) = fs::read_dir(&root) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("python") {
                    let py_dir = entry.path().join("dist-packages");
                    let py_dir_site = entry.path().join("site-packages");

                    for sub_dir in &[py_dir, py_dir_site] {
                        if let Ok(sub_entries) = fs::read_dir(sub_dir) {
                            for sub_entry in sub_entries.flatten() {
                                let sub_name = sub_entry.file_name().to_string_lossy().into_owned();
                                if !sub_name.starts_with('_')
                                    && !sub_name.contains('.')
                                    && !sub_name.contains('-')
                                {
                                    libs.push(sub_name);
                                } else if sub_name.ends_with(".py") && !sub_name.starts_with('_') {
                                    libs.push(sub_name.trim_end_matches(".py").to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    libs.sort();
    libs.dedup();
    libs
}

fn scan_cpp_libraries() -> Vec<String> {
    let mut libs = Vec::new();
    let search_roots = vec![
        PathBuf::from("/usr/include"),
        PathBuf::from("/usr/local/include"),
    ];

    for root in search_roots {
        if let Ok(entries) = fs::read_dir(&root) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with('.') {
                    if entry.path().is_dir() {
                        libs.push(name);
                    } else if name.ends_with(".h") || name.ends_with(".hpp") {
                        let clean = name.split('.').next().unwrap_or(&name).to_string();
                        libs.push(clean);
                    }
                }
            }
        }
    }

    libs.sort();
    libs.dedup();
    libs
}

fn scan_arduino_libraries() -> Vec<String> {
    let mut libs = Vec::new();
    let home = std::env::var("HOME").unwrap_or_default();
    let search_roots = vec![
        PathBuf::from(format!("{}/.arduino15/packages", home)),
        PathBuf::from(format!("{}/Arduino/libraries", home)),
    ];

    for root in search_roots {
        if let Ok(entries) = fs::read_dir(&root) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with('.') && entry.path().is_dir() {
                    libs.push(name);
                }
            }
        }
    }

    libs.sort();
    libs.dedup();
    libs
}

#[derive(Debug, Default)]
pub struct MySystemUtilsService;

#[tonic::async_trait]
impl SystemUtils for MySystemUtilsService {
    async fn restart_daemon(&self, _req: Request<Empty>) -> Result<Response<ApiStatus>, Status> {
        let mut stop_cmd = crate::utils::apt::create_ros2_command("", &["daemon", "stop"]).await;
        let stop_out = stop_cmd.output().await;

        match stop_out {
            Ok(stop_res) => {
                println!("ros2 daemon stop exited with status: {:?}", stop_res.status);
            }
            Err(e) => {
                eprintln!("Failed to stop ros2 daemon: {:?}", e);
            }
        }

        let mut start_cmd = crate::utils::apt::create_ros2_command("", &["daemon", "start"]).await;
        let start_out = start_cmd.output().await;

        match start_out {
            Ok(_) => Ok(Response::new(ApiStatus {
                ok: true,
                code: 0,
                message: "ROS2 daemon restarted successfully".to_string(),
                details: HashMap::new(),
            })),
            Err(e) => Ok(Response::new(ApiStatus {
                ok: false,
                code: 13,
                message: format!("Failed to start ROS2 daemon: {}", e),
                details: HashMap::new(),
            })),
        }
    }

    type RunCommandStream = tokio_stream::wrappers::ReceiverStream<Result<CommandOutput, Status>>;
    async fn run_command(
        &self,
        _req: Request<CommandRequest>,
    ) -> Result<Response<Self::RunCommandStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn start_ssh_session(
        &self,
        _req: Request<SshConfig>,
    ) -> Result<Response<SshSession>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    type ExecRemoteStream = tokio_stream::wrappers::ReceiverStream<Result<CommandOutput, Status>>;
    async fn exec_remote(
        &self,
        _req: Request<RemoteExecRequest>,
    ) -> Result<Response<Self::ExecRemoteStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_available_libraries(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<AvailableLibrariesResponse>, Status> {
        let python_libraries = scan_python_libraries();
        let cpp_libraries = scan_cpp_libraries();
        let arduino_libraries = scan_arduino_libraries();

        Ok(Response::new(AvailableLibrariesResponse {
            python_libraries,
            cpp_libraries,
            arduino_libraries,
        }))
    }
}
