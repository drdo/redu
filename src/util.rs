pub fn snapshot_short_id(id: &str) -> String {
    id.chars().take(7).collect::<String>()
}
