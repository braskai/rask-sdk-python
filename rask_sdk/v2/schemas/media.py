import datetime
import uuid
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Union

import pydantic
from rask_sdk.v2 import enums


class MetaBase(pydantic.BaseModel):
    size_bytes: pydantic.StrictInt
    original_meta: Optional[None] = None


class AudioMeta(MetaBase):
    duration_seconds: pydantic.StrictInt
    audio_rate: pydantic.StrictInt
    audio_layout: Literal["mono", "stereo"]
    audio_channels: pydantic.StrictInt
    audio_codec_name: pydantic.StrictStr


class VideoMeta(AudioMeta):
    video_codec_name: pydantic.StrictStr
    video_frame_rate: pydantic.StrictInt
    video_frame_width: pydantic.StrictInt
    video_frame_height: pydantic.StrictInt


class ImageMeta(MetaBase):
    image_width: pydantic.StrictInt
    image_height: pydantic.StrictInt
    image_format: pydantic.StrictStr


class MediaBase(pydantic.BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    preview_id: Optional[uuid.UUID] = None
    path: str
    name: str
    url: Optional[pydantic.AnyUrl] = None
    kind: enums.MediaKind
    status: enums.MediaStatus
    meta: Union[Dict, Union[VideoMeta, AudioMeta, ImageMeta, Dict]]
    mime_type: pydantic.StrictStr
    created_at: datetime.datetime
    updated_at: datetime.datetime
    deleted_at: Optional[datetime.datetime] = None


class MediaCreateLink(pydantic.BaseModel):
    link: pydantic.AnyUrl
    kind: Optional[enums.MediaKind] = None
    name: Optional[str] = None


class MediaSlimGet(MediaBase):
    meta: Union[VideoMeta, AudioMeta, ImageMeta, Dict]


class MediaGet(MediaSlimGet):
    preview: Optional[MediaSlimGet] = None
