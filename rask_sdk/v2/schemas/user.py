import pydantic


class CreditItemGet(pydantic.BaseModel):
    total: int
    used: int


class CreditsGet(pydantic.BaseModel):
    minutes: CreditItemGet
    video: CreditItemGet
    lipsync_free_minutes: CreditItemGet
