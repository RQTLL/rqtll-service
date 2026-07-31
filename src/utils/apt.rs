use std::collections::HashSet;
use tokio::process::Command;

pub async fn get_ros_distro() -> String {
    if let Ok(entries) = std::fs::read_dir("/opt/ros") {
        let mut found_distros = Vec::new();
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        let setup_path = entry.path().join("setup.bash");
                        if setup_path.exists() {
                            found_distros.push(name.to_string());
                        }
                    }
                }
            }
        }
        
        if !found_distros.is_empty() {
            found_distros.sort();
            // Try to source the first found distro to confirm it works
            let distro = &found_distros[0];
            let output = Command::new("/bin/bash")
                .arg("-c")
                .arg(format!("source /opt/ros/{}/setup.bash && echo $ROS_DISTRO", distro))
                .output()
                .await;

            match output {
                Ok(out) => {
                    let d = String::from_utf8_lossy(&out.stdout).trim().to_string();
                    if !d.is_empty() {
                        return d;
                    }
                }
                Err(_) => {}
            }
            return distro.clone();
        }
    }
    "Ninguna".into()
}

pub async fn get_all_installed_matching_prefixes() -> HashSet<String> {
    let mut installed = HashSet::new();
    let output = Command::new("dpkg-query")
        .args(["-W", "-f=${Package} ${Status}\n", "ros-*", "rti-*", "python3-*"])
        .output()
        .await;

    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            if line.contains("install ok installed") {
                if let Some(name) = line.split_whitespace().next() {
                    installed.insert(name.to_string());
                }
            }
        }
    }
    installed
}

pub async fn check_if_installed(pkg: &str) -> bool {
    let output = Command::new("dpkg-query")
        .args(["-W", "-f=${Status}", pkg])
        .output()
        .await;

    output
        .map(|o| String::from_utf8_lossy(&o.stdout).contains("install ok installed"))
        .unwrap_or(false)
}

pub fn get_configured_domain_id() -> Option<String> {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/akey".to_string());
    
    let username = std::env::var("USER").unwrap_or_else(|_| "akey".to_string());
    let mut default_rc = ".bashrc";
    if let Ok(passwd) = std::fs::read_to_string("/etc/passwd") {
        for line in passwd.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 7 && parts[0] == username {
                if parts[6].contains("zsh") {
                    default_rc = ".zshrc";
                }
                break;
            }
        }
    }
    
    let path = std::path::PathBuf::from(&home).join(default_rc);
    if let Ok(content) = std::fs::read_to_string(&path) {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("export ROS_DOMAIN_ID=") {
                let val = trimmed["export ROS_DOMAIN_ID=".len()..].trim();
                let val = val.split('#').next().unwrap_or(val).trim();
                return Some(val.to_string());
            }
        }
    }
    
    let fallback_rc = if default_rc == ".bashrc" { ".zshrc" } else { ".bashrc" };
    let path = std::path::PathBuf::from(&home).join(fallback_rc);
    if let Ok(content) = std::fs::read_to_string(&path) {
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("export ROS_DOMAIN_ID=") {
                let val = trimmed["export ROS_DOMAIN_ID=".len()..].trim();
                let val = val.split('#').next().unwrap_or(val).trim();
                return Some(val.to_string());
            }
        }
    }
    
    None
}

pub async fn load_ros_environment(ws_path: &str) {
    let distro = get_ros_distro().await;
    let sys_setup = format!("/opt/ros/{}/setup.bash", distro);
    let mut source_parts = Vec::new();
    
    if let Some(domain_id) = get_configured_domain_id() {
        source_parts.push(format!("export ROS_DOMAIN_ID={}", domain_id));
        std::env::set_var("ROS_DOMAIN_ID", &domain_id);
    }
    
    if std::path::Path::new(&sys_setup).exists() {
        source_parts.push(format!("source {}", sys_setup));
    }
    if !ws_path.is_empty() {
        let ws_setup = std::path::Path::new(ws_path).join("install/setup.bash");
        if ws_setup.exists() {
            source_parts.push(format!("source {}", ws_setup.to_string_lossy()));
        }
    }
    
    let source_cmd = if source_parts.is_empty() {
        "env".to_string()
    } else {
        format!("{} && env", source_parts.join(" && "))
    };
    
    let output = Command::new("bash")
        .arg("-c")
        .arg(&source_cmd)
        .output()
        .await;
        
    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        for line in stdout.lines() {
            if let Some(idx) = line.find('=') {
                let key = &line[..idx];
                let value = &line[idx+1..];
                if key.starts_with("ROS_") || 
                   key.starts_with("AMENT_") || 
                   key == "PATH" || 
                   key == "LD_LIBRARY_PATH" || 
                   key == "PYTHONPATH" || 
                   key.starts_with("GZ_") || 
                   key.starts_with("GAZEBO_") ||
                   key == "CMAKE_PREFIX_PATH" {
                    std::env::set_var(key, value);
                }
            }
        }
    }
}

pub async fn create_ros2_command(ws_path: &str, args: &[&str]) -> Command {
    load_ros_environment(ws_path).await;
    let mut cmd = Command::new("ros2");
    cmd.args(args);
    cmd
}
