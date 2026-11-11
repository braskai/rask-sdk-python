import datetime
import uuid
from typing import Dict
from typing import List
from typing import Optional

import pydantic
from rask_sdk.v2 import enums


TIMESTAMP_FORMAT = "%H:%M:%S,%f"
SPEAKER_ID_PREFIX = "SPEAKER_"


def validate_start_end_timestamps(start: str, end: str) -> None:
    """Validate start / end timestamps."""

    try:
        start_dt = datetime.datetime.strptime(start, TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp format for segment start: {start}.") from exc

    try:
        end_dt = datetime.datetime.strptime(end, TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise ValueError(f"Invalid timestamp format for segment end: {end}.") from exc

    if start_dt >= end_dt:
        raise ValueError("Segment start must be less than segment end.")


def validate_speaker(speaker: Optional[str]) -> None:
    """Validate segment speaker."""

    if speaker is None:
        return

    if not speaker.startswith(SPEAKER_ID_PREFIX):
        raise ValueError(f"Speaker {speaker} must start with {SPEAKER_ID_PREFIX}.")

    try:
        int(speaker.replace(SPEAKER_ID_PREFIX, ""))
    except ValueError:
        raise ValueError(
            f"Speaker {speaker} contains invalid characters. Speaker example: {SPEAKER_ID_PREFIX}00"
        )


class ProjectGetSlim(pydantic.BaseModel):
    id: uuid.UUID
    name: str
    source_type: enums.ProjectSourceType
    status: Optional[enums.ProjectStatus] = None
    status_updated_at: Optional[datetime.datetime] = None
    created_at: Optional[datetime.datetime] = None


class ProjectGet(ProjectGetSlim):
    cover: Optional[str] = None
    src_lang: Optional[str] = None
    dst_lang: Optional[str] = None
    num_speakers: Optional[int] = None
    source_url: Optional[str] = None
    duration: Optional[int] = None
    original_duration: Optional[int] = None
    glossary_id: Optional[uuid.UUID] = None
    glossary_version: Optional[int] = None
    original_video: Optional[str] = None
    transcript_id: Optional[uuid.UUID] = None
    voiceover: Optional[str] = None
    translated_video: Optional[str] = None
    translated_audio: Optional[str] = None
    voice: Optional[Dict[str, Optional[str]]] = None
    translation_srt_path: Optional[str] = None
    translation_vtt_path: Optional[str] = None


class ProjectsGet(pydantic.BaseModel):
    total: int
    offset: int
    projects: List[ProjectGetSlim]


class ProjectCreate(pydantic.BaseModel):
    video_id: uuid.UUID
    name: Optional[str] = None
    src_lang: Optional[str] = None
    dst_lang: str
    num_speakers: Optional[int] = None
    transcript_id: Optional[uuid.UUID] = None
    glossary_id: Optional[uuid.UUID] = None


class ProjectPatch(pydantic.BaseModel):
    name: Optional[str] = None
    num_speakers: Optional[int] = None
    voice: Optional[Dict[str, uuid.UUID]] = None

    @pydantic.model_validator(mode="before")
    def validate_voice(cls, values: Dict) -> Dict:
        """Validate voice entities."""

        voice = values.get("voice")

        if not voice:
            return values

        for speaker, _voice_id in voice.items():
            validate_speaker(speaker=speaker)

        return values


class Voice(pydantic.BaseModel):
    id: uuid.UUID
    label: str
    sample_src: Optional[str] = None
    gender: str


class SegmentTextCreatePatch(pydantic.BaseModel):
    text: str
    lang: str


class SegmentCreate(pydantic.BaseModel):
    src: Optional[SegmentTextCreatePatch] = None
    dst: Optional[SegmentTextCreatePatch] = None
    speaker: Optional[str] = None
    start: str
    end: str

    @pydantic.model_validator(mode="before")
    def validate_segment(cls, values: Dict) -> Dict:
        """Validate and clean start / end timestamps and speaker."""

        start = values.get("start")
        end = values.get("end")
        speaker = values.get("speaker")

        validate_speaker(speaker=speaker)

        if start is None or not isinstance(start, str):
            raise ValueError(f"Invalid timestamp format for segment start: {start}.")

        if end is None or not isinstance(end, str):
            raise ValueError(f"Invalid timestamp format for segment end: {end}.")

        start = start.strip()
        end = end.strip()

        if speaker is not None:
            speaker = speaker.strip()

        validate_start_end_timestamps(start=start, end=end)

        if values.get("src") is None and values.get("dst") is None:
            raise ValueError("At least one of src or dst must be specified.")

        values["start"] = start
        values["end"] = end
        values["speaker"] = speaker

        return values


class TranscriptionCreate(pydantic.BaseModel):
    segments: List[SegmentCreate]

    @pydantic.model_validator(mode="after")
    def validate_segment_speakers(self) -> "TranscriptionCreate":
        """Validate segments speakers."""

        speakers = {segment.speaker for segment in self.segments}

        all_speakers_missing = all(speaker is None for speaker in speakers)
        all_speakers_set = all(speaker is not None for speaker in speakers)

        if not (all_speakers_missing or all_speakers_set):
            raise ValueError("Either all or none speakers should be specified.")

        return self


class SegmentTextGet(pydantic.BaseModel):
    text: str
    lang: Optional[str] = None


class SegmentGet(pydantic.BaseModel):
    id: uuid.UUID
    src: Optional[SegmentTextGet] = None
    dst: Optional[SegmentTextGet] = None
    speaker: Optional[str] = None
    start: str
    end: str
    status: enums.SegmentStatus


class SegmentPatch(pydantic.BaseModel):
    id: uuid.UUID
    src: Optional[SegmentTextCreatePatch] = None
    dst: Optional[SegmentTextCreatePatch] = None
    speaker: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None

    @pydantic.model_validator(mode="before")
    def validate_segment(cls, values: dict) -> dict:
        """Validate and clean start / end timestamps and speaker."""

        start = values.get("start")
        end = values.get("end")
        speaker = values.get("speaker")

        validate_speaker(speaker=speaker)

        if start is None and end is None:
            return values

        if start is None or end is None:
            raise ValueError(
                "Both start and end should be specified explicitly to patch segment timestamps."
            )

        if not isinstance(start, str):
            raise ValueError(f"Invalid timestamp format for segment start: {start}.")

        if not isinstance(end, str):
            raise ValueError(f"Invalid timestamp format for segment end: {end}.")

        start = start.strip()
        end = end.strip()

        if speaker is not None:
            speaker = speaker.strip()

        validate_start_end_timestamps(start=start, end=end)

        values["start"] = start
        values["end"] = end
        values["speaker"] = speaker

        return values


class SegmentId(pydantic.BaseModel):
    id: uuid.UUID


class TranscriptionGet(pydantic.BaseModel):
    segments: List[SegmentGet]


class TranscriptionId(TranscriptionGet):
    id: uuid.UUID


class TranscriptionSegmentsCreate(pydantic.BaseModel):
    segments: List[SegmentCreate]


class TranscriptionSegmentsPatch(pydantic.BaseModel):
    segments: List[SegmentPatch]


class CheckFaceTaskResponse(pydantic.BaseModel):
    check_face_task_status: Optional[enums.LipsyncStatus] = None


class LipsyncTaskData(pydantic.BaseModel):
    is_multiple_speakers: Optional[bool] = None
    is_free_lipsync: Optional[bool] = None


class LipsyncTaskResponse(pydantic.BaseModel):
    tasks_in_lipsync_queue: Optional[int] = None
    lipsync_task_status: Optional[enums.LipsyncStatus] = None


class LipsyncInfo(pydantic.BaseModel):
    check_face_task_status: Optional[enums.LipsyncStatus] = None
    tasks_in_lipsync_queue: Optional[int] = None
    lipsync_result_path: Optional[str] = None
    lipsync_task_status: Optional[enums.LipsyncStatus] = None
    video_has_face: Optional[bool] = None
    lipsync_task_progress: Optional[int] = None
