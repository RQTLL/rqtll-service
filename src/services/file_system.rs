use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use rqtll_api::rqtll::api::v1::file_service_server::FileService;
use rqtll_api::rqtll::api::v1::{
    PathRequest, ListFilesResponse, FileInfo, ReadFileRequest, FileContent, WriteFileRequest, RenameRequest, Status as ApiStatus
};
use crate::utils::fs::expand_home_dir;

#[derive(Debug, Default)]
pub struct MyFileService;

#[tonic::async_trait]
impl FileService for MyFileService {
    async fn list(
        &self,
        req: Request<PathRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        let req = req.into_inner();
        let path_str = expand_home_dir(&req.path);
        let path = PathBuf::from(&path_str);

        if !path.exists() {
            return Err(Status::not_found(format!("Path does not exist: {path_str}")));
        }

        let mut entries = vec![];
        if path.is_dir() {
            if req.recursive {
                fn visit_dirs(dir: &Path, list: &mut Vec<FileInfo>) -> std::io::Result<()> {
                    if dir.is_dir() {
                        for entry in std::fs::read_dir(dir)? {
                            let entry = entry?;
                            let path = entry.path();
                            
                            // Ignore common build/cache folders to keep it clean and fast
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                if name == ".git" || name == "__pycache__" {
                                    continue;
                                }
                            }
                            
                            let metadata = entry.metadata()?;
                            let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                            
                            list.push(FileInfo {
                                path: path.to_string_lossy().to_string(),
                                is_dir: path.is_dir(),
                                size: metadata.len() as i64,
                                mtime: Some(prost_types::Timestamp::from(mtime)),
                                metadata: std::collections::HashMap::new(),
                                git_status: 0,
                            });
                            
                            if path.is_dir() {
                                let _ = visit_dirs(&path, list);
                            }
                        }
                    }
                    Ok(())
                }
                let _ = visit_dirs(&path, &mut entries);
            } else {
                if let Ok(dir_entries) = std::fs::read_dir(&path) {
                    for entry in dir_entries.filter_map(Result::ok) {
                        let path = entry.path();
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name == ".git" || name == "__pycache__" {
                                continue;
                            }
                        }
                        if let Ok(metadata) = entry.metadata() {
                            let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                            entries.push(FileInfo {
                                path: path.to_string_lossy().to_string(),
                                is_dir: path.is_dir(),
                                size: metadata.len() as i64,
                                mtime: Some(prost_types::Timestamp::from(mtime)),
                                metadata: std::collections::HashMap::new(),
                                git_status: 0,
                            });
                        }
                    }
                }
            }
        } else {
            if let Ok(metadata) = std::fs::metadata(&path) {
                let mtime = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                entries.push(FileInfo {
                    path: path.to_string_lossy().to_string(),
                    is_dir: false,
                    size: metadata.len() as i64,
                    mtime: Some(prost_types::Timestamp::from(mtime)),
                    metadata: std::collections::HashMap::new(),
                    git_status: 0,
                });
            }
        }

        Ok(Response::new(ListFilesResponse {
            entries,
            status: Some(ApiStatus {
                ok: true,
                code: 0,
                message: "Files listed successfully".to_string(),
                details: std::collections::HashMap::new(),
            }),
        }))
    }

    async fn stat(&self, _req: Request<ReadFileRequest>) -> Result<Response<FileInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn read(&self, req: Request<ReadFileRequest>) -> Result<Response<FileContent>, Status> {
        let req = req.into_inner();
        let path_str = expand_home_dir(&req.path);
        let path = PathBuf::from(&path_str);

        if !path.exists() {
            return Err(Status::not_found(format!("File not found: {path_str}")));
        }

        match std::fs::read(&path) {
            Ok(content) => {
                let is_binary = content.iter().take(8000).any(|&b| b == 0);
                
                Ok(Response::new(FileContent {
                    content,
                    encoding: "utf-8".to_string(),
                    is_binary,
                    status: Some(ApiStatus {
                        ok: true,
                        code: 0,
                        message: "File read successfully".to_string(),
                        details: HashMap::new(),
                    }),
                }))
            }
            Err(e) => {
                Ok(Response::new(FileContent {
                    content: vec![],
                    encoding: String::new(),
                    is_binary: false,
                    status: Some(ApiStatus {
                        ok: false,
                        code: 13,
                        message: format!("Error reading file: {}", e),
                        details: HashMap::new(),
                    }),
                }))
            }
        }
    }

    async fn write(&self, req: Request<WriteFileRequest>) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let path_str = expand_home_dir(&req.path);
        let path = PathBuf::from(&path_str);

        if req.create_dirs {
            if let Some(parent) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return Ok(Response::new(ApiStatus {
                        ok: false,
                        code: 13,
                        message: format!("Error creating parent directories: {}", e),
                        details: HashMap::new(),
                    }));
                }
            }
        }

        match std::fs::write(&path, &req.content) {
            Ok(_) => {
                Ok(Response::new(ApiStatus {
                    ok: true,
                    code: 0,
                    message: "File written successfully".to_string(),
                    details: HashMap::new(),
                }))
            }
            Err(e) => {
                Ok(Response::new(ApiStatus {
                    ok: false,
                    code: 13,
                    message: format!("Error writing file: {}", e),
                    details: HashMap::new(),
                }))
            }
        }
    }

    async fn delete(&self, _req: Request<ReadFileRequest>) -> Result<Response<ApiStatus>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn rename(&self, _req: Request<RenameRequest>) -> Result<Response<ApiStatus>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    type WatchStream = tokio_stream::wrappers::ReceiverStream<Result<rqtll_api::rqtll::api::v1::FileEvent, Status>>;

    async fn watch(&self, _req: Request<PathRequest>) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
