use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::introspection_service_server::IntrospectionService;
use rqtll_api::rqtll::api::v1::{
    Empty, ListNodesResponse, ListTopicsResponse, IntrospectionFilter, GraphEvent,
    NodeInfo, TopicInfoExtended, Status as ApiStatus,
};

pub struct MyIntrospectionService;

impl Default for MyIntrospectionService {
    fn default() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl IntrospectionService for MyIntrospectionService {
    async fn list_nodes(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let output = tokio::process::Command::new("ros2")
            .args(&["node", "list"])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to list nodes: {e}")))?;

        let stdout_str = String::from_utf8_lossy(&output.stdout);
        let mut nodes = Vec::new();

        for line in stdout_str.lines() {
            let line = line.trim();
            if !line.is_empty() {
                let parts: Vec<&str> = line.rsplitn(2, '/').collect();
                let (name, ns) = if parts.len() == 2 {
                    (parts[0].to_string(), format!("/{}", parts[1].trim_start_matches('/')))
                } else {
                    (line.to_string(), "/".to_string())
                };
                nodes.push(NodeInfo {
                    name,
                    namespace: ns,
                    executable: "".to_string(),
                    pid: 0,
                    node_id: "".to_string(),
                });
            }
        }

        Ok(Response::new(ListNodesResponse {
            nodes,
            status: Some(ApiStatus {
                ok: true,
                code: 0,
                message: "Nodes listed successfully".to_string(),
                details: HashMap::new(),
            }),
        }))
    }

    async fn list_topics(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let output = tokio::process::Command::new("ros2")
            .args(&["topic", "list"])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to list topics: {e}")))?;

        let stdout_str = String::from_utf8_lossy(&output.stdout);
        let mut topics = Vec::new();

        for line in stdout_str.lines() {
            let topic_name = line.trim();
            if !topic_name.is_empty() {
                let mut message_type = "unknown".to_string();
                let mut publisher_count = 0;
                let mut subscriber_count = 0;

                let info_out = tokio::process::Command::new("ros2")
                    .args(&["topic", "info", topic_name])
                    .output()
                    .await;
                
                if let Ok(out) = info_out {
                    let info_str = String::from_utf8_lossy(&out.stdout);
                    for info_line in info_str.lines() {
                        let info_line = info_line.trim();
                        if info_line.contains("Type:") {
                            message_type = info_line.split("Type:").nth(1).unwrap_or("").trim().to_string();
                        } else if info_line.contains("Publisher count:") {
                            publisher_count = info_line.split("Publisher count:").nth(1).unwrap_or("").trim().parse::<i32>().unwrap_or(0);
                        } else if info_line.contains("Subscription count:") || info_line.contains("Subscriber count:") {
                            subscriber_count = info_line.split(":").nth(1).unwrap_or("").trim().parse::<i32>().unwrap_or(0);
                        }
                    }
                }

                topics.push(TopicInfoExtended {
                    name: topic_name.to_string(),
                    message_type,
                    publisher_count,
                    subscriber_count,
                    qos_profiles: vec![],
                });
            }
        }

        Ok(Response::new(ListTopicsResponse {
            topics,
            status: Some(ApiStatus {
                ok: true,
                code: 0,
                message: "Topics listed successfully".to_string(),
                details: HashMap::new(),
            }),
        }))
    }

    type WatchGraphStream = tokio_stream::wrappers::ReceiverStream<Result<GraphEvent, Status>>;

    async fn watch_graph(
        &self,
        _req: Request<IntrospectionFilter>,
    ) -> Result<Response<Self::WatchGraphStream>, Status> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}
