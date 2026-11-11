from enum import Enum


class ProjectSourceType(str, Enum):
    """Project source type."""

    YOUTUBE = "youtube"
    GDRIVE = "gdrive"
    VIMEO = "vimeo"
    S3 = "s3"
    ANY = "any"
    LOCAL = "local"


class ProjectStatus(str, Enum):
    """Project status."""

    CREATED = "created"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    UPLOAD_FAILED = "upload_failed"
    TRANSCRIPTION_STARTED = "transcription_started"
    TRANSCRIPTION_DONE = "transcription_done"
    TRANSCRIPTION_FAILED = "transcription_failed"
    TRANSCRIBE_SEGMENTS_STARTED = "transcribe_segments_started"
    TRANSCRIBE_SEGMENTS_DONE = "transcribe_segments_done"
    TRANSCRIBE_SEGMENTS_FAILED = "transcribe_segments_failed"
    SEPARATE_BACKGROUND_STARTED = "separate_background_started"
    SEPARATE_BACKGROUND_DONE = "separate_background_done"
    SEPARATE_BACKGROUND_FAILED = "separate_background_failed"
    DETERMINE_SPEAKERS_STARTED = "determine_speakers_started"
    DETERMINE_SPEAKERS_DONE = "determine_speakers_done"
    DETERMINE_SPEAKERS_FAILED = "determine_speakers_failed"
    VOICE_SUGGEST_STARTED = "voice_suggest_started"
    VOICE_SUGGEST_DONE = "voice_suggest_done"
    VOICE_SUGGEST_FAILED = "voice_suggest_failed"
    TRANSLATION_STARTED = "translation_started"
    TRANSLATION_DONE = "translation_done"
    TRANSLATION_FAILED = "translation_failed"
    VOICE_UPDATE_STARTED = "voice_update_started"
    VOICE_UPDATE_DONE = "voice_update_done"
    VOICE_UPDATE_FAILED = "voice_update_failed"
    TRANSCRIPT_EDITED = "transcript_edited"
    VOICE_EDITED = "voice_edited"
    VOICEOVER_STARTED = "voiceover_started"
    VOICEOVER_DONE = "voiceover_done"
    VOICEOVER_FAILED = "voiceover_failed"
    MERGING_STARTED = "merging_started"
    MERGING_DONE = "merging_done"
    MERGING_FAILED = "merging_failed"
    FAILED = "failed"
    NO_AUDIO = "no_audio"
    NO_WORDS = "no_words"
    FORBIDDEN_LINK = "forbidden_link"


class SegmentStatus(str, Enum):
    """Segment status."""

    PROCESSING = "processing"
    UPDATED = "updated"
    DONE = "done"
    ERROR = "error"


class LipsyncStatus(str, Enum):
    """Lipsync status."""

    STARTED = "started"
    DONE = "done"
    FAILED = "failed"
    OUTDATED = "outdated"
