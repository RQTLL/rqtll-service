use tonic::transport::Server;
use tonic_reflection::server::Builder;

mod services;
mod utils;

use services::clone::MyCloneWorkspaceService;
use services::installer::MyROSInstallerService;
use services::package::MyPackageService;
use services::workspace::MyWorkspaceService;
use services::interactive_execution::MyCommandExecutionService;
use services::data_stream::MyDataStreamService;
use services::build::MyBuildService;
use services::introspection::MyIntrospectionService;
use services::execution::MyExecutionService;
use services::file_system::MyFileService;
use services::terminal::MyTerminalService;
use services::system_utils::MySystemUtilsService;
use utils::apt::get_ros_distro;

use rqtll_api::rqtll::api::v1::clone_workspace_service_server::CloneWorkspaceServiceServer;
use rqtll_api::rqtll::api::v1::package_service_server::PackageServiceServer;
use rqtll_api::rqtll::api::v1::ros_installer_service_server::RosInstallerServiceServer;
use rqtll_api::rqtll::api::v1::workspace_service_server::WorkspaceServiceServer;
use rqtll_api::rqtll::api::v1::command_execution_service_server::CommandExecutionServiceServer;
use rqtll_api::rqtll::api::v1::data_stream_service_server::DataStreamServiceServer;
use rqtll_api::rqtll::api::v1::build_service_server::BuildServiceServer;
use rqtll_api::rqtll::api::v1::introspection_service_server::IntrospectionServiceServer;
use rqtll_api::rqtll::api::v1::execution_service_server::ExecutionServiceServer;
use rqtll_api::rqtll::api::v1::file_service_server::FileServiceServer;
use rqtll_api::rqtll::api::v1::terminal_service_server::TerminalServiceServer;
use rqtll_api::rqtll::api::v1::system_utils_server::SystemUtilsServer;
use rqtll_api::rqtll::api::v1::FILE_DESCRIPTOR_SET;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;

    let reflection_service = Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    let pkg_svc = PackageServiceServer::new(MyPackageService::default());
    let clone_svc = CloneWorkspaceServiceServer::new(MyCloneWorkspaceService::default());
    let installer_svc = RosInstallerServiceServer::new(MyROSInstallerService::default());
    let workspace_svc = WorkspaceServiceServer::new(MyWorkspaceService::default());
    let execution_svc = CommandExecutionServiceServer::new(MyCommandExecutionService::default());
    let data_stream_svc = DataStreamServiceServer::new(MyDataStreamService::default());
    let build_svc = BuildServiceServer::new(MyBuildService::default());
    let introspection_svc = IntrospectionServiceServer::new(MyIntrospectionService::default());
    let node_exec_svc = ExecutionServiceServer::new(MyExecutionService::default());
    let file_svc = FileServiceServer::new(MyFileService::default());
    let terminal_svc = TerminalServiceServer::new(MyTerminalService::default());
    let system_utils_svc = SystemUtilsServer::new(MySystemUtilsService::default());

    println!(">_ RQTLL-API Backend");
    println!("   {}@ROS2 {}", addr, get_ros_distro().await);

    Server::builder()
        .add_service(reflection_service)
        .add_service(pkg_svc)
        .add_service(clone_svc)
        .add_service(installer_svc)
        .add_service(workspace_svc)
        .add_service(execution_svc)
        .add_service(data_stream_svc)
        .add_service(build_svc)
        .add_service(introspection_svc)
        .add_service(node_exec_svc)
        .add_service(file_svc)
        .add_service(terminal_svc)
        .add_service(system_utils_svc)
        .serve(addr)
        .await?;

    Ok(())
}
