use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::package_service_server::PackageService;
use rqtll_api::rqtll::api::v1::{InstallProgress, InstallRequest, ListPackagesRequest, PackageInfo};

use crate::utils::admin::run_apt_action_pkexec;
use crate::utils::apt::{check_if_installed, get_all_installed_matching_prefixes, get_ros_distro};

pub struct MyPackageService {
    is_installing: Arc<Mutex<bool>>,
}

impl Default for MyPackageService {
    fn default() -> Self {
        Self {
            is_installing: Arc::new(Mutex::new(false)),
        }
    }
}

#[tonic::async_trait]
impl PackageService for MyPackageService {
    type ListAvailablePackagesStream = ReceiverStream<Result<PackageInfo, Status>>;
    type InstallPackageStream = ReceiverStream<Result<InstallProgress, Status>>;

    async fn list_available_packages(
        &self,
        req: Request<ListPackagesRequest>,
    ) -> Result<Response<Self::ListAvailablePackagesStream>, Status> {
        let req = req.into_inner();
        let user_filter = req.filter;
        
        let mut prefixes = Vec::new();
        if req.show_ros {
            prefixes.push("ros");
        }
        if req.show_python {
            prefixes.push("python3");
        }
        if req.show_rti {
            prefixes.push("rti");
        }
        
        if prefixes.is_empty() {
            prefixes.extend(&["ros", "rti", "python3"]);
        }
        
        let prefix_regex = prefixes.join("|");
        let regex_filter = if user_filter.is_empty() {
            format!(r"^({})-", prefix_regex)
        } else {
            format!(r"^({})-.*{}", prefix_regex, user_filter)
        };

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            let installed_set = get_all_installed_matching_prefixes().await;
            let current_distro = get_ros_distro().await;

            let output = Command::new("apt-cache")
                .args(["search", "--names-only", &regex_filter])
                .output()
                .await;

            if let Ok(out) = output {
                let stdout = String::from_utf8_lossy(&out.stdout);
                for line in stdout.lines() {
                    if let Some((name, desc)) = line.split_once(" - ") {
                        let clean_name = name.trim();
                        let pkg = PackageInfo {
                            name: clean_name.to_string(),
                            description: desc.trim().to_string(),
                            version: current_distro.clone(),
                            is_installed: installed_set.contains(clean_name),
                        };

                        if tx.send(Ok(pkg)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn install_package(
        &self,
        req: Request<InstallRequest>,
    ) -> Result<Response<Self::InstallPackageStream>, Status> {
        let mut lock = self.is_installing.lock().await;
        if *lock {
            return Err(Status::aborted("APT ocupado"));
        }

        *lock = true;
        let is_installing_flag = Arc::clone(&self.is_installing);
        let pkg_name = req.into_inner().package_name;
        let is_installed = check_if_installed(&pkg_name).await;
        let action = if is_installed { "remove" } else { "install" };
        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            let mut child = match run_apt_action_pkexec(action, &pkg_name) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Error spawning pkexec: {e}");
                    let mut lock = is_installing_flag.lock().await;
                    *lock = false;
                    let _ = tx.send(Ok(InstallProgress {
                        log_line: "ERROR_LAUNCH_FAILED".to_string(),
                        progress: 0.0,
                    })).await;
                    return;
                }
            };

            if let Some(stdout) = child.stdout.take() {
                use tokio::io::AsyncBufReadExt;
                let mut reader = tokio::io::BufReader::new(stdout).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    let _ = tx.send(Ok(InstallProgress {
                        log_line: line,
                        progress: 50.0,
                    })).await;
                }
            }

            let status = child.wait().await;

            let mut lock = is_installing_flag.lock().await;
            *lock = false;

            let final_msg = match status {
                Ok(s) if s.success() => "SUCCESS_COMPLETE",
                _ => "ERROR_CANCELLED",
            };

            let _ = tx.send(Ok(InstallProgress {
                log_line: final_msg.to_string(),
                progress: 100.0,
            })).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
