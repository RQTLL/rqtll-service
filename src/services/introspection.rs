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
        let ws_path = {
            if let Ok(lock) = crate::services::workspace::ACTIVE_WORKSPACE.lock() {
                lock.clone().unwrap_or_else(|| "".to_string())
            } else {
                "".to_string()
            }
        };

        let mut cmd = crate::utils::apt::create_ros2_command(&ws_path, &["node", "list"]).await;
        let output = cmd.output()
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
        let ws_path = {
            if let Ok(lock) = crate::services::workspace::ACTIVE_WORKSPACE.lock() {
                lock.clone().unwrap_or_else(|| "".to_string())
            } else {
                "".to_string()
            }
        };

        let mut cmd = crate::utils::apt::create_ros2_command(&ws_path, &["topic", "list", "-t"]).await;
        let output = cmd.output()
            .await
            .map_err(|e| Status::internal(format!("Failed to list topics: {e}")))?;

        let stdout_str = String::from_utf8_lossy(&output.stdout);
        let mut topics = Vec::new();

        for line in stdout_str.lines() {
            let line = line.trim();
            if !line.is_empty() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let name = parts[0].to_string();
                    let message_type = parts[1]
                        .trim_matches(|c| c == '[' || c == ']')
                        .to_string();
                    
                    topics.push(TopicInfoExtended {
                        name,
                        message_type,
                        publisher_count: 0,
                        subscriber_count: 0,
                        qos_profiles: vec![],
                    });
                } else if parts.len() == 1 {
                    topics.push(TopicInfoExtended {
                        name: parts[0].to_string(),
                        message_type: "unknown".to_string(),
                        publisher_count: 0,
                        subscriber_count: 0,
                        qos_profiles: vec![],
                    });
                }
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
