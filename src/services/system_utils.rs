use std::collections::HashMap;
use tonic::{Request, Response, Status};
use rqtll_api::rqtll::api::v1::system_utils_server::SystemUtils;
use rqtll_api::rqtll::api::v1::{
    Empty, Status as ApiStatus, CommandRequest, CommandOutput, SshConfig, SshSession, RemoteExecRequest
};

#[derive(Debug, Default)]
pub struct MySystemUtilsService;

#[tonic::async_trait]
impl SystemUtils for MySystemUtilsService {
    async fn restart_daemon(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ApiStatus>, Status> {
        let stop_out = tokio::process::Command::new("ros2")
            .args(&["daemon", "stop"])
            .output()
            .await;

        match stop_out {
            Ok(stop_res) => {
                println!("ros2 daemon stop exited with status: {:?}", stop_res.status);
            }
            Err(e) => {
                eprintln!("Failed to stop ros2 daemon: {:?}", e);
            }
        }

        let start_out = tokio::process::Command::new("ros2")
            .args(&["daemon", "start"])
            .output()
            .await;

        match start_out {
            Ok(_) => {
                Ok(Response::new(ApiStatus {
                    ok: true,
                    code: 0,
                    message: "ROS2 daemon restarted successfully".to_string(),
                    details: HashMap::new(),
                }))
            }
            Err(e) => {
                Ok(Response::new(ApiStatus {
                    ok: false,
                    code: 13,
                    message: format!("Failed to start ROS2 daemon: {}", e),
                    details: HashMap::new(),
                }))
            }
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
}
