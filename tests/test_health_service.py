from app.services.health import HealthService


async def test_results_are_cached() -> None:
    calls = 0

    async def check() -> tuple[bool, str | None]:
        nonlocal calls
        calls += 1
        return True, None

    service = HealthService({"dep": check}, cache_ttl=60)

    await service.check()
    result = await service.check()

    assert calls == 1
    assert result["dep"].status == "healthy"


async def test_zero_ttl_checks_every_time() -> None:
    calls = 0

    async def check() -> tuple[bool, str | None]:
        nonlocal calls
        calls += 1
        return True, None

    service = HealthService({"dep": check}, cache_ttl=0)

    await service.check()
    await service.check()

    assert calls == 2


async def test_exception_marks_dependency_unhealthy() -> None:
    async def broken() -> tuple[bool, str | None]:
        raise ConnectionError("no route")

    async def ok() -> tuple[bool, str | None]:
        return True, None

    result = await HealthService({"broken": broken, "ok": ok}, cache_ttl=0).check()

    assert result["broken"].status == "unhealthy"
    assert result["broken"].error == "no route"
    assert result["ok"].status == "healthy"
