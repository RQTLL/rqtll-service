use std::collections::HashMap;
use tokio::process::Command;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use rqtll_api::rqtll::api::v1::introspection_service_server::IntrospectionService;
use rqtll_api::rqtll::api::v1::{
    Empty, GraphEvent, IntrospectionFilter, ListGraphResponse, ListNodesResponse,
    ListTopicsResponse, NodeInfo, NodeInfoExtended, Status as ApiStatus,
    TopicInfoExtended, TopicMetricsRequest, TopicMetricsResponse,
};

pub struct MyIntrospectionService;

impl Default for MyIntrospectionService {
    fn default() -> Self {
        Self
    }
}

fn active_workspace_path() -> String {
    if let Ok(lock) = crate::services::workspace::ACTIVE_WORKSPACE.lock() {
        lock.clone().unwrap_or_else(|| "".to_string())
    } else {
        "".to_string()
    }
}

fn api_status(ok: bool, code: i32, message: impl Into<String>) -> ApiStatus {
    ApiStatus {
        ok,
        code,
        message: message.into(),
        details: HashMap::new(),
    }
}

fn node_full_name(node: &NodeInfo) -> String {
    let namespace = node.namespace.trim();
    let name = node.name.trim();

    if namespace.is_empty() || namespace == "/" {
        format!("/{}", name.trim_start_matches('/'))
    } else {
        format!(
            "/{}/{}",
            namespace.trim_start_matches('/'),
            name.trim_start_matches('/')
        )
    }
}

fn parse_section_entries(output: &str, header: &str) -> Vec<String> {
    let mut entries = Vec::new();
    let mut in_section = false;

    for raw_line in output.lines() {
        let line = raw_line.trim();

        if line.eq_ignore_ascii_case(header) {
            in_section = true;
            continue;
        }

        if in_section {
            if line.is_empty() {
                break;
            }

            if line.ends_with(':') && !line.starts_with('/') {
                break;
            }

            let entry = line
                .split_once(':')
                .map(|(value, _)| value.trim())
                .unwrap_or(line)
                .trim();

            if !entry.is_empty() && !entries.iter().any(|existing| existing == entry) {
                entries.push(entry.to_string());
            }
        }
    }

    entries
}

fn parse_node_info(output: &str) -> (Vec<String>, Vec<String>) {
    (
        parse_section_entries(output, "Publishers:"),
        parse_section_entries(output, "Subscribers:"),
    )
}

fn parse_topic_type(output: &str) -> String {
    for raw_line in output.lines() {
        let line = raw_line.trim();
        if line.to_ascii_lowercase().starts_with("type:") {
            return line
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .unwrap_or_else(|| "unknown".to_string());
        }
    }

    "unknown".to_string()
}

fn parse_hz_output(output: &str) -> String {
    for raw_line in output.lines() {
        let line = raw_line.trim();
        let lower = line.to_ascii_lowercase();
        if lower.contains("average rate:") || lower.starts_with("average:") {
            return line
                .split_once(':')
                .map(|(_, value)| value.trim().to_string())
                .unwrap_or_else(|| line.to_string());
        }
    }

    "Inactivo".to_string()
}

fn parse_bw_output(output: &str) -> String {
    for raw_line in output.lines() {
        let line = raw_line.trim();
        let lower = line.to_ascii_lowercase();
        if lower.contains("average:") || lower.contains("/s") || lower.contains("bandwidth") {
            if lower.contains("average:") {
                return line.split_once(':').map(|(_, val)| val.trim().to_string()).unwrap_or_else(|| line.to_string());
            }
            return line.to_string();
        }
    }

    "---".to_string()
}

async fn run_ros2_output(ws_path: &str, args: &[&str]) -> Result<String, Status> {
    let mut cmd = crate::utils::apt::create_ros2_command(ws_path, args).await;
    let output = cmd
        .output()
        .await
        .map_err(|e| Status::internal(format!("Failed to run ros2 {:?}: {e}", args)))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(format!("{}\n{}", stdout, stderr))
}

async fn run_ros2_output_with_timeout(
    ws_path: &str,
    args: &[&str],
    timeout_secs: u64,
) -> Result<String, Status> {
    crate::utils::apt::load_ros_environment(ws_path).await;

    let mut cmd = Command::new("timeout");
    cmd.arg(format!("{}s", timeout_secs));
    cmd.arg("ros2");
    cmd.args(args);

    let output = cmd
        .output()
        .await
        .map_err(|e| Status::internal(format!("Failed to run ros2 {:?}: {e}", args)))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(format!("{}\n{}", stdout, stderr))
}

#[tonic::async_trait]
impl IntrospectionService for MyIntrospectionService {
    async fn list_nodes(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let ws_path = active_workspace_path();

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
        let ws_path = active_workspace_path();

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

    async fn get_graph(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<ListGraphResponse>, Status> {
        let ws_path = active_workspace_path();
        let nodes_response = self.list_nodes(Request::new(Empty {})).await?.into_inner();

        let mut tasks = Vec::new();
        for node in nodes_response.nodes {
            let ws_path = ws_path.clone();
            tasks.push(tokio::spawn(async move {
                let full_name = node_full_name(&node);
                let output = run_ros2_output(&ws_path, &["node", "info", &full_name]).await?;
                let (publications, subscriptions) = parse_node_info(&output);

                Ok::<NodeInfoExtended, Status>(NodeInfoExtended {
                    node: Some(node),
                    publications,
                    subscriptions,
                })
            }));
        }

        let mut nodes = Vec::new();
        let mut skipped = 0usize;

        for task in tasks {
            match task.await {
                Ok(Ok(node)) => nodes.push(node),
                Ok(Err(_)) | Err(_) => skipped += 1,
            }
        }

        let loaded_nodes = nodes.len();

        Ok(Response::new(ListGraphResponse {
            nodes,
            status: Some(api_status(
                true,
                0,
                format!(
                    "Graph loaded successfully ({} nodes, {} skipped)",
                    loaded_nodes,
                    skipped
                ),
            )),
        }))
    }

    async fn get_topic_metrics(
        &self,
        req: Request<TopicMetricsRequest>,
    ) -> Result<Response<TopicMetricsResponse>, Status> {
        let ws_path = active_workspace_path();
        let topic_name = req.into_inner().topic_name.trim().to_string();

        if topic_name.is_empty() {
            return Err(Status::invalid_argument("topic_name is required"));
        }

        let topic_info = run_ros2_output(&ws_path, &["topic", "info", &topic_name]).await?;
        let message_type = parse_topic_type(&topic_info);

        let hz_output = run_ros2_output_with_timeout(&ws_path, &["topic", "hz", &topic_name], 3).await?;
        let bw_output = run_ros2_output_with_timeout(&ws_path, &["topic", "bw", &topic_name], 2).await?;

        Ok(Response::new(TopicMetricsResponse {
            message_type,
            hz: parse_hz_output(&hz_output),
            bw: parse_bw_output(&bw_output),
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
