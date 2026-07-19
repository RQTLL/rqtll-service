use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::Mutex;
use tokio::process::{Child, Command};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use std::process::Stdio;

use portable_pty::{CommandBuilder, NativePtySystem, PtySize, PtySystem};

use rqtll_api::rqtll::api::v1::command_execution_service_server::CommandExecutionService;
use rqtll_api::rqtll::api::v1::{
    ExecutionRequest, ExecutionOutput, ExecutionInput, ExecutionResize,
    ExecutionControl, ExecutionStatus, Status as ApiStatus, ActiveSessionsResponse, Empty,
};

enum SessionProcess {
    Standard {
        child: Arc<Mutex<Child>>,
        stdin: Option<Arc<Mutex<tokio::process::ChildStdin>>>,
    },
    Pty {
        child: Arc<Mutex<Box<dyn portable_pty::Child + Send + Sync>>>,
        writer: Arc<StdMutex<Box<dyn std::io::Write + Send>>>,
    },
}

struct ActiveSession {
    #[allow(dead_code)]
    session_id: String,
    process_type: String,
    process: SessionProcess,
    master: Option<Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>>,
    input_buffer: Arc<Mutex<String>>,
    request: ExecutionRequest,
    senders: Arc<StdMutex<Vec<mpsc::Sender<Result<ExecutionOutput, Status>>>>>,
}

pub struct MyCommandExecutionService {
    sessions: Arc<Mutex<HashMap<String, ActiveSession>>>,
}

impl Default for MyCommandExecutionService {
    fn default() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

fn send_system_notification(title: &str, message: &str) {
    let _ = std::process::Command::new("notify-send")
        .args(&["--app-name", "RQTLL IDE", title, message])
        .spawn();
}

#[tonic::async_trait]
impl CommandExecutionService for MyCommandExecutionService {
    type StartSessionStream = ReceiverStream<Result<ExecutionOutput, Status>>;

    async fn start_session(
        &self,
        req: Request<ExecutionRequest>,
    ) -> Result<Response<Self::StartSessionStream>, Status> {
        let req_raw = req.into_inner();
        let session_id = req_raw.session_id.clone();
        
        // Reattach to existing session if it is already running
        {
            let sessions = self.sessions.lock().await;
            if let Some(session) = sessions.get(&session_id) {
                let (tx, rx) = mpsc::channel(128);
                if let Ok(mut list) = session.senders.lock() {
                    list.push(tx);
                }
                return Ok(Response::new(ReceiverStream::new(rx)));
            }
        }

        let payload = req_raw.payload.clone().ok_or_else(|| Status::invalid_argument("Missing request payload"))?;

        let (cmd_bin, cmd_args, process_type) = match payload {
            rqtll_api::rqtll::api::v1::execution_request::Payload::Rviz2(rviz2) => {
                let mut args = Vec::new();
                if !rviz2.title.is_empty() {
                    args.push("-t".to_string());
                    args.push(rviz2.title);
                }
                if !rviz2.config_path.is_empty() {
                    args.push("-d".to_string());
                    args.push(rviz2.config_path);
                }
                if !rviz2.fixed_frame.is_empty() {
                    args.push("-f".to_string());
                    args.push(rviz2.fixed_frame);
                }
                if !rviz2.image_path.is_empty() {
                    args.push("-s".to_string());
                    args.push(rviz2.image_path);
                }
                if rviz2.fullscreen {
                    args.push("--fullscreen".to_string());
                }
                if rviz2.log {
                    args.push("-l".to_string());
                }
                ("rviz2".to_string(), args, "rviz2".to_string())
            }
            rqtll_api::rqtll::api::v1::execution_request::Payload::Rqt(rqt) => {
                let mut args = Vec::new();
                if !rqt.perspective.is_empty() {
                    args.push("--perspective".to_string());
                    args.push(rqt.perspective);
                }
                if !rqt.perspective_file.is_empty() {
                    args.push("--perspective-file".to_string());
                    args.push(rqt.perspective_file);
                }
                if rqt.ht {
                    args.push("-ht".to_string());
                }
                if rqt.fl {
                    args.push("-fl".to_string());
                }
                ("rqt".to_string(), args, "rqt".to_string())
            }
            rqtll_api::rqtll::api::v1::execution_request::Payload::GzSim(gz) => {
                let mut args = Vec::new();
                args.push("sim".to_string());
                if !gz.sdf_file.is_empty() {
                    args.push(gz.sdf_file);
                }
                if !gz.gui_config.is_empty() {
                    args.push("--gui-config".to_string());
                    args.push(gz.gui_config);
                }
                if gz.server_only {
                    args.push("-s".to_string());
                }
                if gz.gui_only {
                    args.push("-g".to_string());
                }
                if gz.update_rate > 0.0 {
                    args.push("-z".to_string());
                    args.push(gz.update_rate.to_string());
                }
                if gz.seed != 0 {
                    args.push("--seed".to_string());
                    args.push(gz.seed.to_string());
                }
                if gz.wait_for_assets {
                    args.push("--wait-for-assets".to_string());
                }
                if gz.run_on_start {
                    args.push("-r".to_string());
                }
                if !gz.physics_engine.is_empty() {
                    args.push("--physics-engine".to_string());
                    args.push(gz.physics_engine);
                }
                if !gz.render_engine.is_empty() {
                    args.push("--render-engine".to_string());
                    args.push(gz.render_engine);
                }
                if !gz.render_engine_gui.is_empty() {
                    args.push("--render-engine-gui".to_string());
                    args.push(gz.render_engine_gui);
                }
                if !gz.render_engine_server.is_empty() {
                    args.push("--render-engine-server".to_string());
                    args.push(gz.render_engine_server);
                }
                if !gz.render_engine_api_backend.is_empty() {
                    args.push("--render-engine-api-backend".to_string());
                    args.push(gz.render_engine_api_backend);
                }
                if !gz.render_engine_gui_api_backend.is_empty() {
                    args.push("--render-engine-gui-api-backend".to_string());
                    args.push(gz.render_engine_gui_api_backend);
                }
                if !gz.render_engine_server_api_backend.is_empty() {
                    args.push("--render-engine-server-api-backend".to_string());
                    args.push(gz.render_engine_server_api_backend);
                }
                if gz.network_secondaries > 0 {
                    args.push("--network-secondaries".to_string());
                    args.push(gz.network_secondaries.to_string());
                }
                if !gz.network_role.is_empty() {
                    args.push("--network-role".to_string());
                    args.push(gz.network_role);
                }
                if !gz.record.is_empty() {
                    args.push("--record".to_string());
                    args.push(gz.record);
                }
                if !gz.record_path.is_empty() {
                    args.push("--record-path".to_string());
                    args.push(gz.record_path);
                }
                if gz.record_resources {
                    args.push("--record-resources".to_string());
                }
                for topic in gz.record_topics {
                    args.push("--record-topic".to_string());
                    args.push(topic);
                }
                if gz.record_period > 0.0 {
                    args.push("--record-period".to_string());
                    args.push(gz.record_period.to_string());
                }
                if gz.log_overwrite {
                    args.push("--log-overwrite".to_string());
                }
                if gz.log_compress {
                    args.push("--log-compress".to_string());
                }
                if !gz.playback.is_empty() {
                    args.push("--playback".to_string());
                    args.push(gz.playback);
                }
                ("gz".to_string(), args, "gz_sim".to_string())
            }
            rqtll_api::rqtll::api::v1::execution_request::Payload::Ssh(ssh) => {
                let mut args = Vec::new();
                let use_password = !ssh.password.is_empty();
                
                if use_password {
                    args.push("-p".to_string());
                    args.push(ssh.password);
                    args.push("ssh".to_string());
                }

                args.push("-t".to_string());
                args.push("-t".to_string()); // forces PTY allocation

                if !ssh.username.is_empty() {
                    args.push("-l".to_string());
                    args.push(ssh.username);
                }
                if ssh.port > 0 {
                    args.push("-p".to_string());
                    args.push(ssh.port.to_string());
                }
                if !ssh.key_path.is_empty() {
                    args.push("-i".to_string());
                    args.push(ssh.key_path);
                }
                if ssh.verbose {
                    args.push("-v".to_string());
                }
                if ssh.ipv4_only {
                    args.push("-4".to_string());
                }
                if ssh.ipv6_only {
                    args.push("-6".to_string());
                }
                
                args.push(ssh.server);

                let bin = if use_password { "sshpass".to_string() } else { "ssh".to_string() };
                (bin, args, "ssh".to_string())
            }
        };

        let use_pty = process_type == "ssh" || process_type == "gz_sim";

        if use_pty {
            let pty_system = NativePtySystem::default();
            let pair = pty_system.openpty(PtySize {
                rows: 24,
                cols: 80,
                pixel_width: 0,
                pixel_height: 0,
            }).map_err(|e| Status::internal(format!("Failed to open PTY: {e}")))?;

            let mut cmd = CommandBuilder::new(&cmd_bin);
            cmd.args(&cmd_args);
            
            let child = pair.slave.spawn_command(cmd)
                .map_err(|e| {
                    let msg = format!("No se pudo iniciar la sesión {session_id} ({process_type}): {e}");
                    send_system_notification("Error al Iniciar", &msg);
                    Status::internal(msg)
                })?;

            let child_arc = Arc::new(Mutex::new(child));
            let mut reader = pair.master.try_clone_reader()
                .map_err(|e| Status::internal(format!("Failed to clone PTY reader: {e}")))?;
            let writer = pair.master.take_writer()
                .map_err(|e| Status::internal(format!("Failed to take PTY writer: {e}")))?;
            let master_arc = Arc::new(Mutex::new(pair.master));

            let (tx, rx) = mpsc::channel(128);
            let senders_list = Arc::new(StdMutex::new(vec![tx]));

            // Store active session
            {
                let mut sessions = self.sessions.lock().await;
                sessions.insert(
                    session_id.clone(),
                    ActiveSession {
                        session_id: session_id.clone(),
                        process_type: process_type.clone(),
                        process: SessionProcess::Pty {
                            child: Arc::clone(&child_arc),
                            writer: Arc::new(StdMutex::new(writer)),
                        },
                        master: Some(master_arc),
                        input_buffer: Arc::new(Mutex::new(String::new())),
                        request: req_raw,
                        senders: Arc::clone(&senders_list),
                    },
                );
            }

            // Spawn PTY reader task
            let senders_stdout = Arc::clone(&senders_list);
            let session_id_stdout = session_id.clone();
            tokio::task::spawn_blocking(move || {
                let mut buf = [0u8; 1024];
                while let Ok(n) = reader.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let out = ExecutionOutput {
                        session_id: session_id_stdout.clone(),
                        data: buf[..n].to_vec(),
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        is_stderr: false,
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

            // Spawn wait task with try_wait non-blocking loop to avoid deadlocks
            let sessions_map = Arc::clone(&self.sessions);
            let session_id_wait = session_id.clone();
            let child_arc_clone = Arc::clone(&child_arc);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    let mut child_lock = child_arc_clone.lock().await;
                    match child_lock.try_wait() {
                        Ok(Some(_status)) => {
                            break;
                        }
                        Ok(None) => {}
                        Err(_) => {
                            break;
                        }
                    }
                }
                
                let mut map = sessions_map.lock().await;
                map.remove(&session_id_wait);
            });

            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        // Spawn child process (standard command)
        let child = match Command::new(&cmd_bin)
            .args(&cmd_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn() 
        {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("No se pudo iniciar la sesión {session_id} ({process_type}): {e}");
                send_system_notification("Error al Iniciar", &msg);
                return Err(Status::internal(msg));
            }
        };

        let mut child_nonmut = child;
        let child_stdin = child_nonmut.stdin.take().map(|s| Arc::new(Mutex::new(s)));
        let child_stdout = child_nonmut.stdout.take().ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let child_stderr = child_nonmut.stderr.take().ok_or_else(|| Status::internal("Failed to open stderr"))?;

        let session_id_clone = session_id.clone();
        let child_arc = Arc::new(Mutex::new(child_nonmut));
        let (tx, rx) = mpsc::channel(128);
        let senders_list = Arc::new(StdMutex::new(vec![tx]));

        // Store active session
        {
            let mut sessions = self.sessions.lock().await;
            sessions.insert(
                session_id_clone.clone(),
                ActiveSession {
                    session_id: session_id_clone.clone(),
                    process_type: process_type.clone(),
                    process: SessionProcess::Standard {
                        child: Arc::clone(&child_arc),
                        stdin: child_stdin,
                    },
                    master: None,
                    input_buffer: Arc::new(Mutex::new(String::new())),
                    request: req_raw,
                    senders: Arc::clone(&senders_list),
                },
            );
        }

        // Spawn stdout reader task
        let senders_stdout = Arc::clone(&senders_list);
        let session_id_stdout = session_id_clone.clone();
        tokio::spawn(async move {
            let mut reader = child_stdout;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let out = ExecutionOutput {
                    session_id: session_id_stdout.clone(),
                    data: buf[..n].to_vec(),
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    is_stderr: false,
                };
                
                let list = {
                    if let Ok(guard) = senders_stdout.lock() {
                        guard.clone()
                    } else {
                        Vec::new()
                    }
                };
                for tx in list {
                    let _ = tx.send(Ok(out.clone())).await;
                }
                if let Ok(mut guard) = senders_stdout.lock() {
                    guard.retain(|tx| !tx.is_closed());
                }
            }
        });

        // Spawn stderr reader task
        let senders_stderr = Arc::clone(&senders_list);
        let session_id_stderr = session_id_clone.clone();
        tokio::spawn(async move {
            let mut reader = child_stderr;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let out = ExecutionOutput {
                    session_id: session_id_stderr.clone(),
                    data: buf[..n].to_vec(),
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    is_stderr: true,
                };
                
                let list = {
                    if let Ok(guard) = senders_stderr.lock() {
                        guard.clone()
                    } else {
                        Vec::new()
                    }
                };
                for tx in list {
                    let _ = tx.send(Ok(out.clone())).await;
                }
                if let Ok(mut guard) = senders_stderr.lock() {
                    guard.retain(|tx| !tx.is_closed());
                }
            }
        });

        // Spawn wait task with try_wait non-blocking loop to avoid deadlocks
        let sessions_map = Arc::clone(&self.sessions);
        let session_id_wait = session_id_clone.clone();
        let child_arc_clone = Arc::clone(&child_arc);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                let mut child_lock = child_arc_clone.lock().await;
                match child_lock.try_wait() {
                    Ok(Some(_status)) => {
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => {
                        break;
                    }
                }
            }
            
            let mut map = sessions_map.lock().await;
            map.remove(&session_id_wait);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn send_input(
        &self,
        req: Request<ExecutionInput>,
    ) -> Result<Response<ApiStatus>, Status> {
        let input = req.into_inner();
        let sessions = self.sessions.lock().await;
        
        if let Some(session) = sessions.get(&input.session_id) {
            // Security check for SSH terminal inputs
            if session.process_type == "ssh" {
                let mut buf_lock = session.input_buffer.lock().await;
                if let Ok(incoming_str) = std::str::from_utf8(&input.data) {
                    for c in incoming_str.chars() {
                        if c == '\r' || c == '\n' {
                            let cmd = buf_lock.trim().to_lowercase();
                            let is_forbidden = cmd.starts_with("sudo")
                                || cmd.starts_with("su")
                                || cmd.contains("chmod")
                                || cmd.contains("chown")
                                || cmd.contains("passwd");

                            if is_forbidden {
                                buf_lock.clear();
                                // Send Ctrl+C (ASCII 3) to cancel line
                                match &session.process {
                                    SessionProcess::Standard { stdin: Some(stdin_mutex), .. } => {
                                        let mut stdin = stdin_mutex.lock().await;
                                        let _ = stdin.write_all(&[3]).await;
                                        let _ = stdin.flush().await;
                                    }
                                    SessionProcess::Pty { writer, .. } => {
                                        let writer_clone = Arc::clone(writer);
                                        tokio::task::spawn_blocking(move || {
                                            if let Ok(mut w) = writer_clone.lock() {
                                                let _ = w.write_all(&[3]);
                                                let _ = w.flush();
                                            }
                                        }).await.map_err(|e| Status::internal(e.to_string()))?;
                                    }
                                    _ => {}
                                }
                                return Err(Status::permission_denied(
                                    "Comando bloqueado por motivos de seguridad: no se permite elevar privilegios."
                                ));
                            }
                            buf_lock.clear();
                        } else if c == '\x08' || c == '\x7f' {
                            buf_lock.pop();
                        } else {
                            buf_lock.push(c);
                        }
                    }
                }
            }

            match &session.process {
                SessionProcess::Standard { stdin: Some(stdin_mutex), .. } => {
                    let mut stdin = stdin_mutex.lock().await;
                    stdin.write_all(&input.data).await.map_err(|e| {
                        Status::internal(format!("Failed to write to stdin: {e}"))
                    })?;
                    let _ = stdin.flush().await;
                }
                SessionProcess::Pty { writer, .. } => {
                    let writer_clone = Arc::clone(writer);
                    let data = input.data.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Ok(mut w) = writer_clone.lock() {
                            let _ = w.write_all(&data);
                            let _ = w.flush();
                        }
                    }).await.map_err(|e| Status::internal(e.to_string()))?;
                }
                _ => {}
            }

            return Ok(Response::new(ApiStatus {
                ok: true,
                code: 0,
                message: "Success".to_string(),
                details: HashMap::new(),
            }));
        }

        Err(Status::not_found("Session not found"))
    }

    async fn resize_session(
        &self,
        req: Request<ExecutionResize>,
    ) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let sessions = self.sessions.lock().await;
        
        if let Some(session) = sessions.get(&req.session_id) {
            if let Some(ref master_mutex) = session.master {
                let master = master_mutex.lock().await;
                let _ = master.resize(PtySize {
                    rows: req.rows as u16,
                    cols: req.cols as u16,
                    pixel_width: 0,
                    pixel_height: 0,
                });
                return Ok(Response::new(ApiStatus {
                    ok: true,
                    code: 0,
                    message: "Resized successfully".to_string(),
                    details: HashMap::new(),
                }));
            }
        }

        Err(Status::not_found("Session not found or PTY master not available"))
    }

    async fn control_session(
        &self,
        req: Request<ExecutionControl>,
    ) -> Result<Response<ExecutionStatus>, Status> {
        let req = req.into_inner();
        let sessions = self.sessions.lock().await;
        
        if let Some(session) = sessions.get(&req.session_id) {
            match req.action() {
                rqtll_api::rqtll::api::v1::execution_control::Action::Stop |
                rqtll_api::rqtll::api::v1::execution_control::Action::ForceStop => {
                    let exit_code = match &session.process {
                        SessionProcess::Standard { child, .. } => {
                            let mut c = child.lock().await;
                            let _ = c.kill().await;
                            c.wait().await.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1)
                        }
                        SessionProcess::Pty { child, .. } => {
                            let child_clone = Arc::clone(child);
                            let mut c = child_clone.lock().await;
                            let _ = c.kill();
                            let _ = c.wait();
                            0
                        }
                    };
                    return Ok(Response::new(ExecutionStatus {
                        session_id: req.session_id,
                        is_running: false,
                        exit_code,
                        status: Some(ApiStatus {
                            ok: true,
                            code: 0,
                            message: "Killed".to_string(),
                            details: HashMap::new(),
                        }),
                    }));
                }
                rqtll_api::rqtll::api::v1::execution_control::Action::Restart => {
                    return Err(Status::unimplemented("Restart is not directly supported via control"));
                }
            }
        }

        Ok(Response::new(ExecutionStatus {
            session_id: req.session_id,
            is_running: false,
            exit_code: -1,
            status: Some(ApiStatus {
                ok: false,
                code: 5, // Not Found
                message: "Session not found".to_string(),
                details: HashMap::new(),
            }),
        }))
    }

    async fn get_active_sessions(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ActiveSessionsResponse>, Status> {
        let sessions = self.sessions.lock().await;
        let active_requests = sessions.values()
            .map(|s| s.request.clone())
            .collect();
        
        Ok(Response::new(ActiveSessionsResponse {
            sessions: active_requests,
        }))
    }
}
