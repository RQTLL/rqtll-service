use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tokio::io::AsyncReadExt;

use rqtll_api::rqtll::api::v1::data_stream_service_server::DataStreamService;
use rqtll_api::rqtll::api::v1::{
    SubscribeRequest, TopicMessage, PublishRequest,
    RecordRequest, RecordEvent, PlayRequest, PlaybackEvent,
    Status as ApiStatus, LogEntry, LogLevel,
};



pub struct MyDataStreamService;

impl Default for MyDataStreamService {
    fn default() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl DataStreamService for MyDataStreamService {
    type SubscribeStream = ReceiverStream<Result<TopicMessage, Status>>;

    async fn subscribe(
        &self,
        req: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = req.into_inner();
        let topic_name = req.topic.clone();
        let msg_type = req.message_type.clone();

        let is_image = topic_name == "image";

        if !is_image && !topic_name.is_empty() {
            let info_out = tokio::process::Command::new("ros2")
                .args(&["topic", "info", &topic_name])
                .output()
                .await;
            let info_str = match info_out {
                Ok(out) => String::from_utf8_lossy(&out.stdout).to_string(),
                Err(e) => format!("Failed to get topic info: {e}"),
            };

            let mut echo_child = tokio::process::Command::new("ros2")
                .args(&["topic", "echo", &topic_name])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|e| Status::internal(format!("Failed to run ros2 topic echo: {e}")))?;

            let mut bw_child = tokio::process::Command::new("ros2")
                .args(&["topic", "bw", &topic_name])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|e| Status::internal(format!("Failed to run ros2 topic bw: {e}")))?;

            let mut hz_child = tokio::process::Command::new("ros2")
                .args(&["topic", "hz", &topic_name])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()
                .map_err(|e| Status::internal(format!("Failed to run ros2 topic hz: {e}")))?;

            let echo_stdout = echo_child.stdout.take().ok_or_else(|| Status::internal("Failed to open echo stdout"))?;
            let bw_stdout = bw_child.stdout.take().ok_or_else(|| Status::internal("Failed to open bw stdout"))?;
            let hz_stdout = hz_child.stdout.take().ok_or_else(|| Status::internal("Failed to open hz stdout"))?;

            let (tx, rx) = mpsc::channel(64);

            let echo_pid = echo_child.id();
            let bw_pid = bw_child.id();
            let hz_pid = hz_child.id();

            // Spawn a monitor task to terminate all children if the client cancels the stream
            let tx_monitor = tx.clone();
            tokio::spawn(async move {
                tx_monitor.closed().await;
                if let Some(p) = echo_pid {
                    let _ = std::process::Command::new("kill").args(&["-2", &p.to_string()]).status();
                }
                if let Some(p) = bw_pid {
                    let _ = std::process::Command::new("kill").args(&["-2", &p.to_string()]).status();
                }
                if let Some(p) = hz_pid {
                    let _ = std::process::Command::new("kill").args(&["-2", &p.to_string()]).status();
                }
            });

            let info_base = info_str.clone();

            // Send initial info immediately
            let mut init_meta = HashMap::new();
            init_meta.insert("info".to_string(), info_base.clone());
            init_meta.insert("echo".to_string(), "".to_string());
            let init_msg = TopicMessage {
                timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                topic: topic_name.clone(),
                message_type: msg_type.clone(),
                data: vec![],
                meta: init_meta,
            };
            let _ = tx.send(Ok(init_msg)).await;

            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut echo_reader = tokio::io::BufReader::new(echo_stdout).lines();
                let mut bw_reader = tokio::io::BufReader::new(bw_stdout).lines();
                let mut hz_reader = tokio::io::BufReader::new(hz_stdout).lines();

                let mut current_echo_msg = Vec::new();
                let mut latest_bw = "Bandwidth: ---".to_string();
                let mut latest_hz = "Frecuencia: ---".to_string();
                let mut skipping_data = false;

                loop {
                    tokio::select! {
                        res = echo_reader.next_line() => {
                            match res {
                                Ok(Some(line)) => {
                                    let trimmed = line.trim();
                                    if trimmed == "---" {
                                        skipping_data = false;
                                        if !current_echo_msg.is_empty() {
                                            let msg_content = current_echo_msg.join("\n");
                                            let mut info_val = info_base.clone();
                                            info_val.push_str("\n");
                                            info_val.push_str(&latest_bw);
                                            info_val.push_str("\n");
                                            info_val.push_str(&latest_hz);

                                            let mut meta = HashMap::new();
                                            meta.insert("info".to_string(), info_val);
                                            meta.insert("echo".to_string(), msg_content.clone());

                                            let topic_msg = TopicMessage {
                                                timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                                                topic: topic_name.clone(),
                                                message_type: msg_type.clone(),
                                                data: msg_content.into_bytes(),
                                                meta,
                                            };
                                            if tx.send(Ok(topic_msg)).await.is_err() {
                                                break;
                                            }
                                            current_echo_msg.clear();
                                        }
                                    } else {
                                        if trimmed.starts_with("data:") {
                                            current_echo_msg.push(line.clone());
                                            let indent = line.len() - trimmed.len();
                                            let spaces = " ".repeat(indent);
                                            current_echo_msg.push(format!("{}  ...", spaces));
                                            skipping_data = true;
                                        } else if skipping_data {
                                            if trimmed.starts_with("-") || trimmed.chars().all(|c| c.is_numeric() || c == '.' || c == '-' || c == ',' || c == ' ') {
                                                // Skip
                                            } else {
                                                skipping_data = false;
                                                current_echo_msg.push(line);
                                            }
                                        } else {
                                            current_echo_msg.push(line);
                                        }
                                    }
                                }
                                _ => break,
                            }
                        }
                        res = bw_reader.next_line() => {
                            match res {
                                Ok(Some(line)) => {
                                    if line.contains("/s from") {
                                        latest_bw = line;
                                        let mut info_val = info_base.clone();
                                        info_val.push_str("\n");
                                        info_val.push_str(&latest_bw);
                                        info_val.push_str("\n");
                                        info_val.push_str(&latest_hz);

                                        let mut meta = HashMap::new();
                                        meta.insert("info".to_string(), info_val);
                                        meta.insert("echo".to_string(), "".to_string());

                                        let topic_msg = TopicMessage {
                                            timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                                            topic: topic_name.clone(),
                                            message_type: msg_type.clone(),
                                            data: vec![],
                                            meta,
                                        };
                                        if tx.send(Ok(topic_msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        res = hz_reader.next_line() => {
                            match res {
                                Ok(Some(line)) => {
                                    if line.contains("average rate:") {
                                        latest_hz = line;
                                        let mut info_val = info_base.clone();
                                        info_val.push_str("\n");
                                        info_val.push_str(&latest_bw);
                                        info_val.push_str("\n");
                                        info_val.push_str(&latest_hz);

                                        let mut meta = HashMap::new();
                                        meta.insert("info".to_string(), info_val);
                                        meta.insert("echo".to_string(), "".to_string());

                                        let topic_msg = TopicMessage {
                                            timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                                            topic: topic_name.clone(),
                                            message_type: msg_type.clone(),
                                            data: vec![],
                                            meta,
                                        };
                                        if tx.send(Ok(topic_msg)).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }

                // Hard-terminate all processes on loop break to prevent leaks
                if let Some(p) = echo_pid {
                    let _ = std::process::Command::new("kill").args(&["-9", &p.to_string()]).status();
                }
                if let Some(p) = bw_pid {
                    let _ = std::process::Command::new("kill").args(&["-9", &p.to_string()]).status();
                }
                if let Some(p) = hz_pid {
                    let _ = std::process::Command::new("kill").args(&["-9", &p.to_string()]).status();
                }
            });

            return Ok(Response::new(ReceiverStream::new(rx)));
        }
        let output = tokio::process::Command::new("ros2")
            .args(&["topic", "list"])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to list topics: {e}")))?;
        let stdout_str = String::from_utf8_lossy(&output.stdout);
        let topics: Vec<&str> = stdout_str.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();

        let mut image_topics = Vec::new();
        for t in topics {
            let info_out = tokio::process::Command::new("ros2")
                .args(&["topic", "info", t])
                .output()
                .await;
            if let Ok(out) = info_out {
                let info_str = String::from_utf8_lossy(&out.stdout);
                for line in info_str.lines() {
                    if line.contains("Type:") {
                        let t_type = line.split("Type:").nth(1).unwrap_or("").trim();
                        if t_type.contains("sensor_msgs/msg/Image") {
                            image_topics.push((t.to_string(), false));
                            break;
                        } else if t_type.contains("sensor_msgs/msg/CompressedImage") {
                            image_topics.push((t.to_string(), true));
                            break;
                        }
                    }
                }
            }
        }

        if image_topics.is_empty() {
            return Err(Status::not_found("No image topics found"));
        }

        let mut prio_1 = Vec::new();
        let mut prio_2 = Vec::new();
        let mut prio_3 = Vec::new();

        for (name, is_comp) in image_topics {
            let lower = name.to_lowercase();
            if lower.contains("compressed") || lower.contains("processed") {
                prio_1.push((name, is_comp));
            } else if lower.contains("raw") {
                prio_3.push((name, is_comp));
            } else {
                prio_2.push((name, is_comp));
            }
        }

        let mut sorted = prio_1;
        sorted.extend(prio_2);
        sorted.extend(prio_3);

        let (selected_topic, is_compressed) = &sorted[0];

        let mut child = tokio::process::Command::new("python3")
            .arg("src/services/image_bridge.py")
            .arg(selected_topic)
            .arg(if *is_compressed { "true" } else { "false" })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to spawn image bridge: {e}")))?;

        let mut stdout = child.stdout.take().ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            loop {
                if let Err(_) = stdout.read_exact(&mut len_buf).await {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut data_buf = vec![0u8; len];
                if let Err(_) = stdout.read_exact(&mut data_buf).await {
                    break;
                }

                let msg = TopicMessage {
                    timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                    topic: "image".to_string(),
                    message_type: "sensor_msgs/msg/Image".to_string(),
                    data: data_buf,
                    meta: HashMap::new(),
                };

                if tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
            let _ = child.kill().await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn publish(
        &self,
        req: Request<PublishRequest>,
    ) -> Result<Response<ApiStatus>, Status> {
        let req = req.into_inner();
        let topic = req.topic.clone();
        let msg_type = req.message_type.clone();
        let mut data_str = String::from_utf8_lossy(&req.data).to_string();
        
        // Clean leading/trailing quotes if they were added in the UI
        data_str = data_str.trim().to_string();
        if (data_str.starts_with('\'') && data_str.ends_with('\'')) ||
           (data_str.starts_with('"') && data_str.ends_with('"')) {
            data_str = data_str[1..data_str.len()-1].trim().to_string();
        }

        println!("[gRPC Backend] Publish Request: topic={}, msg_type={}, data={}", topic, msg_type, data_str);

        tokio::spawn(async move {
            let mut cmd = tokio::process::Command::new("ros2");
            cmd.args(&[
                "topic", "pub", 
                "-1", 
                "--max-wait-time-secs", "2", 
                &topic, &msg_type, &data_str
            ]);
            let _ = cmd.spawn();
        });

        Ok(Response::new(ApiStatus {
            ok: true,
            code: 0,
            message: "Published".to_string(),
            details: HashMap::new(),
        }))
    }

    type RecordStream = ReceiverStream<Result<RecordEvent, Status>>;

    async fn record(
        &self,
        req: Request<RecordRequest>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        let req = req.into_inner();
        let mut args = vec!["bag".to_string(), "record".to_string()];
        if !req.output_path.is_empty() {
            args.push("-o".to_string());
            args.push(req.output_path.clone());
        }
        if req.record_all {
            args.push("-a".to_string());
        } else {
            for t in req.topics {
                args.push(t);
            }
        }

        let mut child = tokio::process::Command::new("ros2")
            .args(&args)
            .env("RCUTILS_COLORIZED_OUTPUT", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to run ros2 bag record: {e}")))?;

        let stdout = child.stdout.take().ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| Status::internal("Failed to open stderr"))?;

        let (tx, rx) = mpsc::channel(64);
        let pid = child.id();

        let tx_monitor = tx.clone();
        tokio::spawn(async move {
            tx_monitor.closed().await;
            if let Some(p) = pid {
                let _ = std::process::Command::new("kill")
                    .args(&["-2", &p.to_string()])
                    .status();
            }
        });

        let tx_out = tx.clone();
        tokio::spawn(async move {
            let mut reader = stdout;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = RecordEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::record_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Info as i32,
                        source: "ros2_bag_record".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_out.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        let tx_err = tx.clone();
        tokio::spawn(async move {
            let mut reader = stderr;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = RecordEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::record_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Warn as i32,
                        source: "ros2_bag_record".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_err.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let exit_status = child.wait().await;
            let success = match exit_status {
                Ok(status) => status.success(),
                Err(_) => false,
            };
            let final_event = RecordEvent {
                ev: Some(rqtll_api::rqtll::api::v1::record_event::Ev::Status(ApiStatus {
                    ok: success,
                    code: if success { 0 } else { 2 },
                    message: if success { "Recording completed successfully" } else { "Recording process stopped" }.to_string(),
                    details: HashMap::new(),
                })),
            };
            let _ = tx.send(Ok(final_event)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type PlayStream = ReceiverStream<Result<PlaybackEvent, Status>>;

    async fn play(
        &self,
        req: Request<PlayRequest>,
    ) -> Result<Response<Self::PlayStream>, Status> {
        let req = req.into_inner();
        let mut args = vec!["bag".to_string(), "play".to_string(), req.path.clone()];
        if req.rate > 0.0 {
            args.push("-r".to_string());
            args.push(req.rate.to_string());
        }
        if req.r#loop {
            args.push("-l".to_string());
        }

        let mut child = tokio::process::Command::new("ros2")
            .args(&args)
            .env("RCUTILS_COLORIZED_OUTPUT", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| Status::internal(format!("Failed to run ros2 bag play: {e}")))?;

        let stdout = child.stdout.take().ok_or_else(|| Status::internal("Failed to open stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| Status::internal("Failed to open stderr"))?;

        let (tx, rx) = mpsc::channel(64);
        let pid = child.id();

        let tx_monitor = tx.clone();
        tokio::spawn(async move {
            tx_monitor.closed().await;
            if let Some(p) = pid {
                let _ = std::process::Command::new("kill")
                    .args(&["-2", &p.to_string()])
                    .status();
            }
        });

        let tx_out = tx.clone();
        tokio::spawn(async move {
            let mut reader = stdout;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = PlaybackEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::playback_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Info as i32,
                        source: "ros2_bag_play".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_out.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        let tx_err = tx.clone();
        tokio::spawn(async move {
            let mut reader = stderr;
            let mut buf = [0u8; 1024];
            while let Ok(n) = reader.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                let event = PlaybackEvent {
                    ev: Some(rqtll_api::rqtll::api::v1::playback_event::Ev::Log(LogEntry {
                        timestamp: Some(prost_types::Timestamp::from(std::time::SystemTime::now())),
                        level: LogLevel::Warn as i32,
                        source: "ros2_bag_play".to_string(),
                        message: chunk,
                        session_id: "".to_string(),
                    })),
                };
                if tx_err.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let exit_status = child.wait().await;
            let success = match exit_status {
                Ok(status) => status.success(),
                Err(_) => false,
            };
            let final_event = PlaybackEvent {
                ev: Some(rqtll_api::rqtll::api::v1::playback_event::Ev::Status(ApiStatus {
                    ok: success,
                    code: if success { 0 } else { 2 },
                    message: if success { "Playback completed successfully" } else { "Playback process stopped" }.to_string(),
                    details: HashMap::new(),
                })),
            };
            let _ = tx.send(Ok(final_event)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
