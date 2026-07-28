use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tokio::io::AsyncReadExt;

use rqtll_api::rqtll::api::v1::data_stream_service_server::DataStreamService;
use rqtll_api::rqtll::api::v1::{
    SubscribeRequest, TopicMessage, PublishRequest,
    RecordRequest, RecordEvent, PlayRequest, PlaybackEvent,
    Status as ApiStatus,
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
        _req: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
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
        let data_str = String::from_utf8_lossy(&req.data).to_string();

        tokio::spawn(async move {
            let mut cmd = tokio::process::Command::new("ros2");
            cmd.args(&["topic", "pub", "-1", &topic, &msg_type, &data_str]);
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
        _req: Request<RecordRequest>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        Err(Status::unimplemented("Record is not implemented"))
    }

    type PlayStream = ReceiverStream<Result<PlaybackEvent, Status>>;

    async fn play(
        &self,
        _req: Request<PlayRequest>,
    ) -> Result<Response<Self::PlayStream>, Status> {
        Err(Status::unimplemented("Play is not implemented"))
    }
}
