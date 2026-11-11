import uuid
from http import HTTPStatus
from json import JSONDecodeError
from typing import BinaryIO
from typing import Dict
from typing import List
from typing import Optional

from anyio import Lock  # httpx brings anyio, and Authlib takes its own lock from there
from authlib.integrations.base_client import OAuthError  # type: ignore[import-untyped]
from authlib.integrations.httpx_client import AsyncOAuth2Client  # type: ignore[import-untyped]
from httpx import HTTPStatusError
from httpx import Response
from rask_sdk.v2 import enums
from rask_sdk.v2 import schemas
from rask_sdk.v2.exceptions.base import RaskClientException
from rask_sdk.v2.utils import retry_on_auth_error


class RaskSDKClient:
    """Rask SDK Client."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        """."""

        self._base_url = "https://api.rask.ai"
        self._client = AsyncOAuth2Client(
            f"{client_id}",
            f"{client_secret}",
            token_endpoint_auth_method="client_secret_post",
            grant_type="client_credentials",
            scope=["api/source", "api/input", "api/output", "api/limit"],
            token_endpoint="https://rask-prod.auth.us-east-2.amazoncognito.com/oauth2/token",
        )
        self._auth_lock = Lock()

    async def aclose(self) -> None:
        """Close the HTTP client and the connections it holds."""

        await self._client.aclose()

    async def __aenter__(self) -> "RaskSDKClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    @staticmethod
    def _raise_for_status(response: Response) -> None:
        """Raise Rask Client Exception on HTTP errors occurred."""

        try:
            response.raise_for_status()
        except HTTPStatusError as exc:
            try:
                err_detail = response.json()
            except JSONDecodeError:
                raise RaskClientException(
                    status_code=response.status_code, detail="Unknown error occurred."
                ) from exc

            # A proxy may answer with a JSON list, string or null, which has no detail to read.
            if not isinstance(err_detail, dict):
                err_detail = {}

            raise RaskClientException(
                status_code=response.status_code,
                detail=err_detail.get("detail", "Unknown error occurred."),
            ) from exc

    async def authenticate(self) -> None:
        """Fetch new token for the instantiated client."""

        try:
            await self._client.fetch_token()
        except OAuthError as exc:
            raise RaskClientException(
                status_code=HTTPStatus.UNAUTHORIZED, detail="Authentication error occurred."
            ) from exc

    # Users
    @retry_on_auth_error()
    async def get_credits(self) -> schemas.CreditsGet:
        """Get credits of the current user."""

        credits_ = await self._client.get(f"{self._base_url}/v2/credits")
        self._raise_for_status(response=credits_)

        return schemas.CreditsGet.model_validate(obj=credits_.json())

    # Media
    @retry_on_auth_error()
    async def create_media_file(
        self, file: BinaryIO, kind: Optional[enums.MediaKind] = None, timeout: int = 1800
    ) -> schemas.MediaGet:
        """Create media by binary file provided."""

        media = await self._client.post(
            f"{self._base_url}/api/library/v1/media",
            files={"data": file},
            params={"kind": kind.value if kind is not None else None},
            timeout=timeout,
        )
        self._raise_for_status(response=media)

        return schemas.MediaGet.model_validate(obj=media.json())

    @retry_on_auth_error()
    async def create_media_link(self, data: schemas.MediaCreateLink) -> schemas.MediaGet:
        """Create media by link provided."""

        media = await self._client.post(
            f"{self._base_url}/api/library/v1/media/link",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=media)

        return schemas.MediaGet.model_validate(obj=media.json())

    @retry_on_auth_error()
    async def get_media(self, media_id: uuid.UUID) -> schemas.MediaGet:
        """Get media by id."""

        media = await self._client.get(
            f"{self._base_url}/api/library/v1/media/{str(media_id)}"
        )
        self._raise_for_status(response=media)

        return schemas.MediaGet.model_validate(obj=media.json())

    # Projects
    @retry_on_auth_error()
    async def create_project(self, data: schemas.ProjectCreate) -> schemas.ProjectGet:
        """Create project."""

        project = await self._client.post(
            f"{self._base_url}/v2/projects",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=project)

        return schemas.ProjectGet.model_validate(obj=project.json())

    @retry_on_auth_error()
    async def get_project(self, project_id: uuid.UUID) -> schemas.ProjectGet:
        """Get project by id."""

        project = await self._client.get(f"{self._base_url}/v2/projects/{str(project_id)}")
        self._raise_for_status(response=project)

        return schemas.ProjectGet.model_validate(obj=project.json())

    @retry_on_auth_error()
    async def get_projects(
        self, offset: int = 0, limit: int = 10, name: Optional[str] = None
    ) -> schemas.ProjectsGet:
        """Get projects."""

        projects = await self._client.get(
            f"{self._base_url}/v2/projects",
            params={
                "offset": offset,
                "limit": limit,
                "name": name,
            },
        )
        self._raise_for_status(response=projects)

        return schemas.ProjectsGet.model_validate(obj=projects.json())

    @retry_on_auth_error()
    async def generate_project(self, project_id: uuid.UUID) -> schemas.ProjectGet:
        """Generate project."""

        project = await self._client.post(
            f"{self._base_url}/v2/projects/{str(project_id)}/generate",
        )
        self._raise_for_status(response=project)

        return schemas.ProjectGet.model_validate(obj=project.json())

    @retry_on_auth_error()
    async def patch_project(
        self, project_id: uuid.UUID, data: schemas.ProjectPatch
    ) -> schemas.ProjectGet:
        """Patch project."""

        project = await self._client.patch(
            f"{self._base_url}/v2/projects/{str(project_id)}",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=project)

        return schemas.ProjectGet.model_validate(obj=project.json())

    @retry_on_auth_error()
    async def get_project_voices(self, project_id: uuid.UUID) -> List[schemas.Voice]:
        """Get project voices."""

        voices = await self._client.get(
            f"{self._base_url}/v2/projects/{str(project_id)}/voices"
        )
        self._raise_for_status(response=voices)

        return [schemas.Voice.model_validate(obj=voice) for voice in voices.json()]

    # Lipsync
    @retry_on_auth_error()
    async def run_check_face_task(
        self, project_id: uuid.UUID
    ) -> schemas.CheckFaceTaskResponse:
        """Run check face task for project id provided."""

        response = await self._client.put(
            f"{self._base_url}/v2/projects/{str(project_id)}/check_face",
        )
        self._raise_for_status(response=response)

        return schemas.CheckFaceTaskResponse.model_validate(obj=response.json())

    @retry_on_auth_error()
    async def run_lipsync_task(
        self,
        project_id: uuid.UUID,
        data: schemas.LipsyncTaskData,
    ) -> schemas.LipsyncTaskResponse:
        """Run lipsync task for project id provided."""

        response = await self._client.put(
            f"{self._base_url}/v2/projects/{str(project_id)}/lipsync",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=response)

        return schemas.LipsyncTaskResponse.model_validate(obj=response.json())

    @retry_on_auth_error()
    async def get_lipsync_info(self, project_id: uuid.UUID) -> schemas.LipsyncInfo:
        """ "Get lipsync info."""

        info = await self._client.get(
            f"{self._base_url}/v2/projects/{str(project_id)}/lipsync",
        )
        self._raise_for_status(response=info)

        return schemas.LipsyncInfo.model_validate(obj=info.json())

    # Transcriptions
    @retry_on_auth_error()
    async def create_transcription(
        self, data: schemas.TranscriptionCreate
    ) -> schemas.TranscriptionId:
        """Create transcription."""

        transcription = await self._client.post(
            f"{self._base_url}/v2/transcriptions",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=transcription)

        return schemas.TranscriptionId.model_validate(obj=transcription.json())

    @retry_on_auth_error()
    async def create_transcription_srt(
        self,
        src: Optional[BinaryIO] = None,
        dst: Optional[BinaryIO] = None,
        src_lang: Optional[str] = None,
        dst_lang: Optional[str] = None,
        timeout: int = 1800,
    ) -> schemas.TranscriptionId:
        """Create transcription via .srt uploading."""

        files: Dict[str, BinaryIO] = {}
        params: Dict[str, str] = {}

        if src is not None:
            files["src"] = src

        if dst is not None:
            files["dst"] = dst

        if src_lang is not None:
            params["src_lang"] = src_lang

        if dst_lang is not None:
            params["dst_lang"] = dst_lang

        transcription = await self._client.post(
            f"{self._base_url}/v2/transcriptions/srt",
            files=files,
            params=params,
            timeout=timeout,
        )
        self._raise_for_status(response=transcription)

        return schemas.TranscriptionId.model_validate(obj=transcription.json())

    @retry_on_auth_error()
    async def get_project_transcription(
        self,
        project_id: uuid.UUID,
        segment_ids: Optional[List[uuid.UUID]] = None,
    ) -> schemas.TranscriptionGet:
        """Get transcription associated with the project."""

        transcription = await self._client.get(
            f"{self._base_url}/v2/projects/{str(project_id)}/transcription",
            params={"segment_ids": segment_ids} if segment_ids else None,
        )
        self._raise_for_status(response=transcription)

        return schemas.TranscriptionGet.model_validate(obj=transcription.json())

    @retry_on_auth_error()
    async def add_project_transcription_segments(
        self, project_id: uuid.UUID, data: schemas.TranscriptionSegmentsCreate
    ) -> schemas.TranscriptionGet:
        """Create segments in the transcription associated with the project."""

        transcription = await self._client.post(
            f"{self._base_url}/v2/projects/{str(project_id)}/transcription/segments",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=transcription)

        return schemas.TranscriptionGet.model_validate(obj=transcription.json())

    @retry_on_auth_error()
    async def patch_project_transcription_segments(
        self, project_id: uuid.UUID, data: schemas.TranscriptionSegmentsPatch
    ) -> schemas.TranscriptionGet:
        """Patch segments in the transcription associated with the project."""

        transcription = await self._client.patch(
            f"{self._base_url}/v2/projects/{str(project_id)}/transcription/segments",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=transcription)

        return schemas.TranscriptionGet.model_validate(obj=transcription.json())

    @retry_on_auth_error()
    async def delete_project_transcription_segment(
        self, project_id: uuid.UUID, segment_id: uuid.UUID
    ) -> schemas.SegmentId:
        """Delete the segment in the transcription associated with the project."""

        segment = await self._client.delete(
            f"{self._base_url}/v2/projects/{str(project_id)}/transcription/segments/{str(segment_id)}",
        )
        self._raise_for_status(response=segment)

        return schemas.SegmentId.model_validate(obj=segment.json())

    # Glossaries
    @retry_on_auth_error()
    async def create_glossary(self, data: schemas.GlossaryCreate) -> schemas.GlossaryGet:
        """Create new glossary."""

        glossary = await self._client.post(
            f"{self._base_url}/v2/glossaries",
            json=data.model_dump(mode="json"),
        )

        self._raise_for_status(response=glossary)

        return schemas.GlossaryGet.model_validate(obj=glossary.json())

    @retry_on_auth_error()
    async def get_glossary(self, glossary_id: uuid.UUID) -> schemas.GlossaryGet:
        """Get glossary by id."""

        glossary = await self._client.get(f"{self._base_url}/v2/glossaries/{str(glossary_id)}")
        self._raise_for_status(response=glossary)

        return schemas.GlossaryGet.model_validate(obj=glossary.json())

    @retry_on_auth_error()
    async def update_glossary(
        self, glossary_id: uuid.UUID, data: schemas.GlossaryUpdate
    ) -> schemas.GlossaryGet:
        """Update existing glossary."""

        glossary = await self._client.put(
            f"{self._base_url}/v2/glossaries/{str(glossary_id)}",
            json=data.model_dump(mode="json"),
        )
        self._raise_for_status(response=glossary)

        return schemas.GlossaryGet.model_validate(obj=glossary.json())

    @retry_on_auth_error()
    async def delete_glossary(self, glossary_id: uuid.UUID) -> schemas.GlossaryIdGet:
        """Delete glossary by id."""

        glossary = await self._client.delete(
            f"{self._base_url}/v2/glossaries/{str(glossary_id)}"
        )
        self._raise_for_status(response=glossary)

        return schemas.GlossaryIdGet.model_validate(obj=glossary.json())
