use std::path::PathBuf;
use tokio::process::Command;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::workspace_service_server::WorkspaceService;
use rqtll_api::rqtll::api::v1::{
    CreateNodesAndLaunchersRequest, CreatePackageRequest, CreateWorkspaceRequest,
    ListWorkspacePackagesRequest, ListWorkspacePackagesResponse, OpenWorkspaceRequest,
    OpenWorkspaceResponse,
};

use crate::utils::apt::get_ros_distro;
use crate::utils::fs::expand_home_dir;

fn find_templates_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("../rqtll-components/templates"),
        PathBuf::from("rqtll-components/templates"),
        PathBuf::from("/home/akey/Proyectos/rqtll/rqtll-components/templates"),
    ];
    for c in &candidates {
        if c.exists() && c.is_dir() {
            return Some(c.clone());
        }
    }
    None
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

#[derive(Debug, Default)]
pub struct MyWorkspaceService;

#[tonic::async_trait]
impl WorkspaceService for MyWorkspaceService {
    async fn open_workspace(
        &self,
        _req: Request<OpenWorkspaceRequest>,
    ) -> Result<Response<OpenWorkspaceResponse>, Status> {
        Ok(Response::new(OpenWorkspaceResponse {
            packages: vec![],
            status: Some(rqtll_api::rqtll::api::v1::Status {
                ok: true,
                code: 0,
                message: "Stub implementation".to_string(),
                details: std::collections::HashMap::new(),
            }),
        }))
    }

    async fn list_packages(
        &self,
        _req: Request<ListWorkspacePackagesRequest>,
    ) -> Result<Response<ListWorkspacePackagesResponse>, Status> {
        Ok(Response::new(ListWorkspacePackagesResponse {
            packages: vec![],
            status: Some(rqtll_api::rqtll::api::v1::Status {
                ok: true,
                code: 0,
                message: "Stub implementation".to_string(),
                details: std::collections::HashMap::new(),
            }),
        }))
    }

    async fn create_workspace(
        &self,
        req: Request<CreateWorkspaceRequest>,
    ) -> Result<Response<rqtll_api::rqtll::api::v1::Status>, Status> {
        let req = req.into_inner();
        let expanded = expand_home_dir(&req.path);
        let path = PathBuf::from(expanded);
        let src_path = path.join("src");

        match std::fs::create_dir_all(&src_path) {
            Ok(_) => Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                ok: true,
                code: 0,
                message: format!("Workspace and src directory created successfully at {:?}", src_path),
                details: std::collections::HashMap::new(),
            })),
            Err(e) => Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                ok: false,
                code: 13, // INTERNAL
                message: format!("Error creating workspace src directory: {}", e),
                details: std::collections::HashMap::new(),
            })),
        }
    }

    async fn create_package(
        &self,
        req: Request<CreatePackageRequest>,
    ) -> Result<Response<rqtll_api::rqtll::api::v1::Status>, Status> {
        let req = req.into_inner();

        let mut args = vec![
            "pkg".to_string(),
            "create".to_string(),
            req.name.clone(),
        ];

        if !req.build_type.is_empty() {
            args.push("--build-type".to_string());
            args.push(req.build_type.clone());
        }

        if let Some(desc) = req.options.get("description") {
            if !desc.trim().is_empty() {
                args.push("--description".to_string());
                args.push(desc.clone());
            }
        }

        if let Some(lic) = req.options.get("license") {
            if !lic.trim().is_empty() {
                args.push("--license".to_string());
                args.push(lic.clone());
            }
        }

        if let Some(email) = req.options.get("maintainer-email") {
            if !email.trim().is_empty() {
                args.push("--maintainer-email".to_string());
                args.push(email.clone());
            }
        }

        if let Some(name) = req.options.get("maintainer-name") {
            if !name.trim().is_empty() {
                args.push("--maintainer-name".to_string());
                args.push(name.clone());
            }
        }

        if let Some(dest) = req.options.get("destination-directory") {
            if !dest.trim().is_empty() {
                let expanded_dest = expand_home_dir(dest);
                args.push("--destination-directory".to_string());
                args.push(expanded_dest);
            }
        }

        if let Some(deps_str) = req.options.get("dependencies") {
            let deps: Vec<&str> = deps_str.split_whitespace().filter(|s| !s.is_empty()).collect();
            if !deps.is_empty() {
                args.push("--dependencies".to_string());
                for dep in deps {
                    args.push(dep.to_string());
                }
            }
        }

        let distro = get_ros_distro().await;
        let script = "source /opt/ros/$1/setup.bash && shift && ros2 \"$@\"";

        let mut cmd = Command::new("bash");
        cmd.arg("-c")
            .arg(script)
            .arg("--") // placeholder for $0
            .arg(&distro);

        for arg in args {
            cmd.arg(arg);
        }

        match cmd.output().await {
            Ok(out) => {
                let stdout = String::from_utf8_lossy(&out.stdout).to_string();
                let stderr = String::from_utf8_lossy(&out.stderr).to_string();
                if out.status.success() {
                    let dest_dir = if let Some(dest) = req.options.get("destination-directory") {
                        expand_home_dir(dest)
                    } else {
                        let expanded_ws = expand_home_dir(&req.workspace_path);
                        PathBuf::from(expanded_ws).join("src").to_string_lossy().to_string()
                    };
                    let pkg_dir = PathBuf::from(dest_dir).join(&req.name);

                    if let Some(version) = req.options.get("version") {
                        let version = version.trim();
                        if !version.is_empty() {
                            // 1. Update package.xml
                            let pkg_xml_path = pkg_dir.join("package.xml");
                            if pkg_xml_path.exists() {
                                if let Ok(mut content) = std::fs::read_to_string(&pkg_xml_path) {
                                    content = content.replace("<version>0.0.0</version>", &format!("<version>{}</version>", version));
                                    let _ = std::fs::write(&pkg_xml_path, content);
                                }
                            }

                            // 2. Update setup.py
                            let setup_py_path = pkg_dir.join("setup.py");
                            if setup_py_path.exists() {
                                if let Ok(mut content) = std::fs::read_to_string(&setup_py_path) {
                                    content = content.replace("version='0.0.0',", &format!("version='{}',", version));
                                    let _ = std::fs::write(&setup_py_path, content);
                                }
                            }

                            // 3. Update CMakeLists.txt
                            let cmake_path = pkg_dir.join("CMakeLists.txt");
                            if cmake_path.exists() {
                                if let Ok(mut content) = std::fs::read_to_string(&cmake_path) {
                                    let default_project = format!("project({})", req.name);
                                    let replacement_project = format!("project({} VERSION {})", req.name, version);
                                    content = content.replace(&default_project, &replacement_project);
                                    let _ = std::fs::write(&cmake_path, content);
                                }
                            }

                            // 4. Update Cargo.toml
                            let cargo_path = pkg_dir.join("Cargo.toml");
                            if cargo_path.exists() {
                                if let Ok(mut content) = std::fs::read_to_string(&cargo_path) {
                                    content = content.replace("version = \"0.0.1\"", &format!("version = \"{}\"", version));
                                    let _ = std::fs::write(&cargo_path, content);
                                }
                            }
                        }
                    }

                    Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                        ok: true,
                        code: 0,
                        message: format!("Package created successfully. stdout: {}", stdout),
                        details: std::collections::HashMap::new(),
                    }))
                } else {
                    Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                        ok: false,
                        code: 13, // INTERNAL
                        message: format!("ros2 pkg create failed:\nstdout: {}\nstderr: {}", stdout, stderr),
                        details: std::collections::HashMap::new(),
                    }))
                }
            }
            Err(e) => {
                Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                    ok: false,
                    code: 13, // INTERNAL
                    message: format!("Failed to execute command: {}", e),
                    details: std::collections::HashMap::new(),
                }))
            }
        }
    }
    async fn create_nodes_and_launchers(
        &self,
        req: Request<CreateNodesAndLaunchersRequest>,
    ) -> Result<Response<rqtll_api::rqtll::api::v1::Status>, Status> {
        let req = req.into_inner();
        let templates_dir = match find_templates_dir() {
            Some(d) => d,
            None => {
                return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                    ok: false,
                    code: 5, // NOT_FOUND
                    message: "Templates directory not found".to_string(),
                    details: std::collections::HashMap::new(),
                }));
            }
        };

        let pkg_dir = PathBuf::from(expand_home_dir(&req.workspace_path))
            .join("src")
            .join(&req.package_name);

        if !pkg_dir.exists() {
            return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                ok: false,
                code: 5, // NOT_FOUND
                message: format!("Package directory {:?} does not exist", pkg_dir),
                details: std::collections::HashMap::new(),
            }));
        }

        // Detect build type from package.xml
        let pkg_xml_path = pkg_dir.join("package.xml");
        let pkg_xml_content = std::fs::read_to_string(&pkg_xml_path).unwrap_or_default();
        let build_type = if pkg_xml_content.contains("ament_python") {
            "ament_python"
        } else if pkg_xml_content.contains("ament_cmake") {
            "ament_cmake"
        } else {
            "unknown"
        };

        // 1. Copy nodes
        for node in &req.nodes {
            let node_base = node.strip_suffix(".py").or(node.strip_suffix(".cpp")).unwrap_or(node);
            let template_name = if node.ends_with(".py") {
                if node.contains("subscriber") { "minimal_subscriber.py" } else { "minimal_publisher.py" }
            } else if node.ends_with(".cpp") {
                if node.contains("subscriber") { "minimal_subscriber.cpp" } else { "minimal_publisher.cpp" }
            } else {
                continue;
            };

            let template_path = templates_dir.join(template_name);
            let template_content = match std::fs::read_to_string(&template_path) {
                Ok(content) => content,
                Err(e) => {
                    return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                        ok: false,
                        code: 13,
                        message: format!("Failed to read template {:?}: {}", template_path, e),
                        details: std::collections::HashMap::new(),
                    }));
                }
            };

            let class_name = to_pascal_case(node_base);
            let mut content = template_content.clone();
            content = content.replace("class MinimalPublisher", &format!("class {}", class_name));
            content = content.replace("class MinimalSubscriber", &format!("class {}", class_name));

            if node.ends_with(".py") {
                content = content.replace("super().__init__('minimal_publisher')", &format!("super().__init__('{}')", node_base));
                content = content.replace("super().__init__('minimal_subscriber')", &format!("super().__init__('{}')", node_base));
                content = content.replace("minimal_publisher = MinimalPublisher()", &format!("{} = {}()", node_base, class_name));
                content = content.replace("minimal_subscriber = MinimalSubscriber()", &format!("{} = {}()", node_base, class_name));
                content = content.replace("rclpy.spin(minimal_publisher)", &format!("rclpy.spin({})", node_base));
                content = content.replace("rclpy.spin(minimal_subscriber)", &format!("rclpy.spin({})", node_base));
            } else if node.ends_with(".cpp") {
                content = content.replace(": Node(\"minimal_publisher\")", &format!(": Node(\"{}\")", node_base));
                content = content.replace(": Node(\"minimal_subscriber\")", &format!(": Node(\"{}\")", node_base));
                content = content.replace("std::make_shared<MinimalPublisher>()", &format!("std::make_shared<{}>()", class_name));
                content = content.replace("std::make_shared<MinimalSubscriber>()", &format!("std::make_shared<{}>()", class_name));
            }

            let dest_file_path = if build_type == "ament_python" {
                let dest_dir = pkg_dir.join(&req.package_name);
                let _ = std::fs::create_dir_all(&dest_dir);
                dest_dir.join(node)
            } else {
                let dest_dir = pkg_dir.join("src");
                let _ = std::fs::create_dir_all(&dest_dir);
                dest_dir.join(node)
            };

            if let Err(e) = std::fs::write(&dest_file_path, content) {
                return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                    ok: false,
                    code: 13,
                    message: format!("Failed to write node to {:?}: {}", dest_file_path, e),
                    details: std::collections::HashMap::new(),
                }));
            }
        }

        // 2. Copy launchers
        if !req.launchers.is_empty() {
            let launch_dir = pkg_dir.join("launch");
            if let Err(e) = std::fs::create_dir_all(&launch_dir) {
                return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                    ok: false,
                    code: 13,
                    message: format!("Failed to create launch directory {:?}: {}", launch_dir, e),
                    details: std::collections::HashMap::new(),
                }));
            }

            let exec_name = req.nodes.first()
                .map(|n| n.strip_suffix(".py").or(n.strip_suffix(".cpp")).unwrap_or(n))
                .unwrap_or("talker");

            for launcher in &req.launchers {
                let template_name = if launcher.ends_with(".py") {
                    "sample_launch.py"
                } else if launcher.ends_with(".xml") {
                    "sample_launch.xml"
                } else if launcher.ends_with(".yaml") {
                    "sample_launch.yaml"
                } else {
                    continue;
                };

                let template_path = templates_dir.join(template_name);
                let template_content = match std::fs::read_to_string(&template_path) {
                    Ok(content) => content,
                    Err(e) => {
                        return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                            ok: false,
                            code: 13,
                            message: format!("Failed to read template {:?}: {}", template_path, e),
                            details: std::collections::HashMap::new(),
                        }));
                    }
                };

                let mut content = template_content.clone();
                content = content.replace("demo_nodes_cpp", &req.package_name);
                content = content.replace("talker", exec_name);

                let dest_file_path = launch_dir.join(launcher);
                if let Err(e) = std::fs::write(&dest_file_path, content) {
                    return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                        ok: false,
                        code: 13,
                        message: format!("Failed to write launcher to {:?}: {}", dest_file_path, e),
                        details: std::collections::HashMap::new(),
                    }));
                }
            }
        }

        // 3. Update configurations
        if build_type == "ament_python" {
            let setup_py_path = pkg_dir.join("setup.py");
            if setup_py_path.exists() {
                let mut setup_content = std::fs::read_to_string(&setup_py_path).unwrap_or_default();
                
                if !setup_content.contains("import os") {
                    setup_content = format!("import os\nfrom glob import glob\n{}", setup_content);
                } else if !setup_content.contains("from glob import glob") {
                    setup_content = format!("from glob import glob\n{}", setup_content);
                }

                let launch_entry = "\n        (os.path.join('share', package_name, 'launch'), glob(os.path.join('launch', '*launch.[pxy][yma]*'))),";
                if !setup_content.contains("share/launch") && !setup_content.contains("'launch'") {
                    setup_content = setup_content.replace(
                        "('share/' + package_name, ['package.xml']),",
                        &format!("('share/' + package_name, ['package.xml']),{}", launch_entry)
                    );
                }

                let mut scripts = String::new();
                for node in &req.nodes {
                    let node_base = node.strip_suffix(".py").or(node.strip_suffix(".cpp")).unwrap_or(node);
                    scripts.push_str(&format!("            '{} = {}.{}:main',\n", node_base, req.package_name, node_base));
                }
                if !scripts.is_empty() {
                    setup_content = setup_content.replace(
                        "'console_scripts': [",
                        &format!("'console_scripts': [\n{}", scripts)
                    );
                }

                if let Err(e) = std::fs::write(&setup_py_path, setup_content) {
                    return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                        ok: false,
                        code: 13,
                        message: format!("Failed to write setup.py: {}", e),
                        details: std::collections::HashMap::new(),
                    }));
                }
            }
        } else if build_type == "ament_cmake" {
            let cmake_path = pkg_dir.join("CMakeLists.txt");
            if cmake_path.exists() {
                let mut cmake_content = std::fs::read_to_string(&cmake_path).unwrap_or_default();
                let mut cmake_block = String::new();
                cmake_block.push_str("\n# Add C++ executables and map dependencies\n");

                let mut deps = vec!["rclcpp".to_string(), "std_msgs".to_string()];
                for line in pkg_xml_content.lines() {
                    if let Some(start) = line.find("<depend>") {
                        if let Some(end) = line.find("</depend>") {
                            let dep = line[start + 8..end].trim().to_string();
                            if !deps.contains(&dep) {
                                deps.push(dep);
                            }
                        }
                    }
                }
                let deps_str = deps.join(" ");

                let mut execs = Vec::new();
                for node in &req.nodes {
                    if node.ends_with(".cpp") {
                        let node_base = node.strip_suffix(".cpp").unwrap_or(node);
                        execs.push(node_base.to_string());
                        cmake_block.push_str(&format!("add_executable({} src/{})\n", node_base, node));
                        cmake_block.push_str(&format!("ament_target_dependencies({} {})\n\n", node_base, deps_str));
                    }
                }

                if !execs.is_empty() {
                    cmake_block.push_str("install(TARGETS\n");
                    for exec in execs {
                        cmake_block.push_str(&format!("  {}\n", exec));
                    }
                    cmake_block.push_str("  DESTINATION lib/${PROJECT_NAME}\n)\n\n");
                }

                if !req.launchers.is_empty() {
                    cmake_block.push_str("# Install launch files\n");
                    cmake_block.push_str("install(DIRECTORY launch\n  DESTINATION share/${PROJECT_NAME}\n)\n\n");
                }

                if cmake_content.contains("ament_package()") {
                    cmake_content = cmake_content.replace("ament_package()", &format!("{}ament_package()", cmake_block));
                    if let Err(e) = std::fs::write(&cmake_path, cmake_content) {
                        return Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
                            ok: false,
                            code: 13,
                            message: format!("Failed to write CMakeLists.txt: {}", e),
                            details: std::collections::HashMap::new(),
                        }));
                    }
                }
            }
        }

        Ok(Response::new(rqtll_api::rqtll::api::v1::Status {
            ok: true,
            code: 0,
            message: "Nodes and launchers created successfully".to_string(),
            details: std::collections::HashMap::new(),
        }))
    }
}
