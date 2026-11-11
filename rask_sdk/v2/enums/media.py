from enum import Enum


class MediaKind(str, Enum):
    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"


class MediaStatus(str, Enum):
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"
