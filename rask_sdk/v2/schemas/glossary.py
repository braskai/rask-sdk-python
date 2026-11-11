import re
import uuid
from typing import Dict
from typing import Optional

import pydantic


MAX_WORD_SIZE = 1024  # 1024bytes
MAX_GLOSSARY_SIZE = 10485760  # 10mb


def validate_entries(
    entries: Optional[Dict[str, str]], max_glossary_size: int = MAX_GLOSSARY_SIZE
) -> Optional[Dict[str, str]]:
    if entries is None:
        return entries

    total_size = 0
    for key, value in entries.items():
        if not key.strip() or not value.strip():
            raise ValueError(
                f"Neither the source nor the target word can be empty for key {key}."
            )

        if re.search(r"[\t\n]", key) or re.search(r"[\t\n]", value):
            raise ValueError(
                f"Special characters like tabulation (\\t) or newline (\\n) are not allowed for key {key}."
            )

        if (
            re.match(r"^\s", key)
            or re.search(r"\s$", key)
            or re.match(r"^\s", value)
            or re.search(r"\s$", value)
        ):
            raise ValueError(
                f"Leading or trailing Unicode whitespace characters are not allowed for key {key}."
            )

        # Check for maximum size of each source/target text
        if (
            len(key.encode("utf-8")) > MAX_WORD_SIZE
            or len(value.encode("utf-8")) > MAX_WORD_SIZE
        ):
            raise ValueError(
                f"Max size for each source/target text is {MAX_WORD_SIZE} UTF-8 bytes for key {key}."
            )

        # Account for the size of each key-value pair in the total size of the dictionary
        total_size += len(key.encode("utf-8")) + len(value.encode("utf-8"))

    # Check for maximum size of the dictionary
    if total_size >= max_glossary_size:
        raise ValueError(f"Maximum dictionary size is {max_glossary_size} UTF-8 bytes.")

    return entries


class GlossaryCreate(pydantic.BaseModel):
    name: str
    src_lang: str
    dst_lang: str
    entries: Dict[str, str]

    @pydantic.field_validator("entries", mode="before")
    @classmethod
    def validate_entries(cls, entries: Dict[str, str]) -> Optional[Dict[str, str]]:
        return validate_entries(entries)

    @pydantic.field_validator("name")
    @classmethod
    def strip_whitespace(cls, value: str) -> str:
        return value.strip()


class GlossaryUpdate(pydantic.BaseModel):
    name: str
    entries: Dict[str, str]

    @pydantic.field_validator("entries", mode="before")
    @classmethod
    def validate_entries(cls, entries: Dict[str, str]) -> Optional[Dict[str, str]]:
        return validate_entries(entries)

    @pydantic.field_validator("name")
    @classmethod
    def strip_whitespace(cls, value: str) -> str:
        return value.strip()


class GlossaryGet(pydantic.BaseModel):
    id: uuid.UUID
    name: str
    version: int
    src_lang: str
    dst_lang: str
    entries: Dict[str, str]


class GlossaryIdGet(pydantic.BaseModel):
    id: uuid.UUID
