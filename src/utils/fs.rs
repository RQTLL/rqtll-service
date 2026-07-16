use std::path::PathBuf;

pub fn expand_home_dir(input: &str) -> String {
    if let Some(rest) = input.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{home}/{rest}");
        }
    }

    if input == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return home;
        }
    }

    input.to_string()
}

pub fn workspace_state_file() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".local")
        .join("share")
        .join("rqtll")
        .join("target_dir.txt")
}
