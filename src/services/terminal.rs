use portable_pty::{Child, CommandBuilder, MasterPty, NativePtySystem, PtySize, PtySystem};
use rqtll_api::rqtll::api::v1::terminal_service_server::TerminalService;
use rqtll_api::rqtll::api::v1::{
    AttachRequest, SessionRequest, StartTerminalRequest, StartTerminalResponse,
    Status as ApiStatus, TerminalInput, TerminalOutput, TerminalResize,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::{mpsc, Mutex};
use tonic::{Request, Response, Status};

pub struct ActiveTerminalSession {
    pub master: Arc<Mutex<Box<dyn MasterPty + Send>>>,
    pub child: Arc<Mutex<Box<dyn Child + Send + Sync>>>,
    pub writer: Arc<Mutex<Box<dyn std::io::Write + Send>>>,
    pub senders: Arc<StdMutex<Vec<mpsc::Sender<Result<TerminalOutput, Status>>>>>,
}

#[derive(Clone)]
pub struct MyTerminalService {
    sessions: Arc<Mutex<HashMap<String, ActiveTerminalSession>>>,
}

impl Default for MyTerminalService {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl TerminalService for MyTerminalService {
    async fn start(
        &self,
        req: Request<StartTerminalRequest>,
    ) -> Result<Response<StartTerminalResponse>, Status> {
        let req = req.into_inner();
        let session_id = uuid::Uuid::new_v4().to_string();

        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/bash".to_string());

        let pty_system = NativePtySystem::default();
        let pair = pty_system
            .openpty(PtySize {
                rows: if req.rows > 0 { req.rows as u16 } else { 24 },
                cols: if req.cols > 0 { req.cols as u16 } else { 80 },
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| Status::internal(format!("Failed to open PTY: {e}")))?;

        let mut cmd = CommandBuilder::new(&shell);
        cmd.env("TERM", "xterm-256color");
        cmd.env("RCUTILS_COLORIZED_OUTPUT", "1");
        cmd.env("CLICOLOR_FORCE", "1");
        cmd.env("FORCE_COLOR", "1");

        for (k, v) in req.env {
            cmd.env(k, v);
        }

        if !req.cwd.is_empty() {
            let expanded_cwd = crate::utils::fs::expand_home_dir(&req.cwd);
            let path = std::path::PathBuf::from(expanded_cwd);
            if path.is_dir() {
                cmd.cwd(path);
            }
        }

        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|e| Status::internal(format!("Failed to spawn shell: {e}")))?;

        let master = pair.master;
        let writer = master
            .take_writer()
            .map_err(|e| Status::internal(format!("Failed to take PTY writer: {e}")))?;

        let master_arc = Arc::new(Mutex::new(master));
        let writer_arc = Arc::new(Mutex::new(writer));
        let child_arc = Arc::new(Mutex::new(child));

        let senders_list = Arc::new(StdMutex::new(Vec::<
            mpsc::Sender<Result<TerminalOutput, Status>>,
        >::new()));

        let senders_stdout = Arc::clone(&senders_list);
        let session_id_stdout = session_id.clone();
        let master_arc_clone = Arc::clone(&master_arc);

        tokio::task::spawn_blocking(move || {
            let mut reader = {
                let master_lock =
                    tokio::runtime::Handle::current().block_on(master_arc_clone.lock());
                match master_lock.try_clone_reader() {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Failed to clone PTY reader: {e}");
                        return;
                    }
                }
            };

            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                let out = TerminalOutput {
                    session_id: session_id_stdout.clone(),
                    data: buf[..n].to_vec(),
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                };

                let list = {
                    if let Ok(guard) = senders_stdout.lock() {
                        guard.clone()
                    } else {
                        Vec::new()
                    }
                };
                for tx in list {
                    let _ = tokio::runtime::Handle::current().block_on(tx.send(Ok(out.clone())));
                }
                if let Ok(mut guard) = senders_stdout.lock() {
                    guard.retain(|tx| !tx.is_closed());
                }
            }
        });

        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(
                session_id.clone(),
                ActiveTerminalSession {
                    master: master_arc,
                    child: child_arc,
                    writer: writer_arc,
                    senders: senders_list,
                },
            );
        }

        Ok(Response::new(StartTerminalResponse {
            session_id,
            status: Some(ApiStatus {
                ok: true,
                code: 0,
                message: "Terminal started successfully".to_string(),
                details: HashMap::new(),
            }),
        }))
    }

    type AttachStream = tokio_stream::wrappers::ReceiverStream<Result<TerminalOutput, Status>>;

    async fn attach(
        &self,
        req: Request<AttachRequest>,
    ) -> Result<Response<Self::AttachStream>, Status> {
        let req = req.into_inner();
        let sessions = self.sessions.lock().await;

        let session = sessions
            .get(&req.session_id)
            .ok_or_else(|| Status::not_found("Terminal session not found"))?;

        let (tx, rx) = mpsc::channel(128);
        if let Ok(mut guard) = session.senders.lock() {
            guard.push(tx);
        }

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn send_input(&self, req: Request<TerminalInput>) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let sessions = self.sessions.lock().await;

        let session = sessions
            .get(&req.session_id)
            .ok_or_else(|| Status::not_found("Terminal session not found"))?;

        let mut writer = session.writer.lock().await;
        writer
            .write_all(&req.data)
            .map_err(|e| Status::internal(format!("Failed to write PTY input: {e}")))?;

        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Input sent".to_string(),
            details: HashMap::new(),
        }))
    }

    async fn resize(&self, req: Request<TerminalResize>) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let sessions = self.sessions.lock().await;

        let session = sessions
            .get(&req.session_id)
            .ok_or_else(|| Status::not_found("Terminal session not found"))?;

        let master = session.master.lock().await;
        master
            .resize(PtySize {
                rows: req.rows as u16,
                cols: req.cols as u16,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|e| Status::internal(format!("Failed to resize PTY: {e}")))?;

        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Terminal resized".to_string(),
            details: HashMap::new(),
        }))
    }

    async fn close(&self, req: Request<SessionRequest>) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let mut sessions = self.sessions.lock().await;

        if let Some(session) = sessions.remove(&req.session_id) {
            let mut child = session.child.lock().await;
            let _ = child.kill();
        }

        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Terminal closed".to_string(),
            details: HashMap::new(),
        }))
    }
}
