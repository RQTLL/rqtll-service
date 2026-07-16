use tonic::transport::Server;
use tonic_reflection::server::Builder;

mod services;
mod utils;

use services::clone::MyCloneWorkspaceService;
use services::installer::MyROSInstallerService;
use services::package::MyPackageService;
use services::workspace::MyWorkspaceService;
use utils::apt::get_ros_distro;

use rqtll_api::rqtll::api::v1::clone_workspace_service_server::CloneWorkspaceServiceServer;
use rqtll_api::rqtll::api::v1::package_service_server::PackageServiceServer;
use rqtll_api::rqtll::api::v1::ros_installer_service_server::RosInstallerServiceServer;
use rqtll_api::rqtll::api::v1::workspace_service_server::WorkspaceServiceServer;
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

    println!(">_ RQTLL-API Backend");
    println!("   {}@ROS2 {}", addr, get_ros_distro().await);

    Server::builder()
        .add_service(reflection_service)
        .add_service(pkg_svc)
        .add_service(clone_svc)
        .add_service(installer_svc)
        .add_service(workspace_svc)
        .serve(addr)
        .await?;

    Ok(())
}
