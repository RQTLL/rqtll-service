use std::process::Stdio;
use tokio::process::{Child, Command};

pub fn run_apt_action_pkexec(action: &str, pkg_name: &str) -> Result<Child, std::io::Error> {
    Command::new("pkexec")
        .args(["apt-get", action, "-y", pkg_name])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

pub fn run_apt_install_sudo(packages: &[&str]) -> Result<Child, std::io::Error> {
    let mut args = vec!["apt-get", "install", "-y"];
    args.extend(packages);
    Command::new("pkexec")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}
