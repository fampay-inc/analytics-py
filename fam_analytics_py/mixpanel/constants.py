class MessageType:
    event = "event"
    profile = "profile"


PAYLOAD_PATH_MAP = {
    MessageType.event: "{base_url}/import?strict=1&project_id={project_id}",
    MessageType.profile: "{base_url}/engage?strict=1#profile-set",
}
