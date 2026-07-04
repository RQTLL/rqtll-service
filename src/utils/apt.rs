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
