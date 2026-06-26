import time


def test_warm_pool_refills_claims_and_shutdown_tears_down(monkeypatch):
    import vmon.pool as pool_mod

    class FakeAgent:
        def ping(self, timeout=None):
            return {"ok": True}

    class FakeVM:
        made = []

        def __init__(self, name):
            self.name = name
            self.stopped = False
            self.removed = False
            FakeVM.made.append(self)

        @classmethod
        def fork_snapshot(cls, template, agent=True):
            return cls(f"fork-{len(cls.made)}")

        @classmethod
        def restore(cls, template, agent=True):
            return cls(f"restore-{len(cls.made)}")

        def agent(self, connect_timeout=None):
            self.connect_timeout = connect_timeout
            return FakeAgent()

        def stop(self):
            self.stopped = True

        def remove(self):
            self.removed = True

    monkeypatch.setattr(pool_mod, "MicroVM", FakeVM)

    pool = pool_mod.WarmPool(template="x", size=2, ping_timeout=0.2)
    try:
        deadline = time.time() + 3.0
        while time.time() < deadline and pool.stats()["ready_count"] < 2:
            time.sleep(0.02)
        assert pool.stats()["ready_count"] == 2

        claimed = pool.claim()
        assert isinstance(claimed, FakeVM)
        assert pool.stats()["hits"] == 1
    finally:
        pool.shutdown()

    parked = [vm for vm in FakeVM.made if vm is not claimed]
    assert parked
    assert all(vm.stopped and vm.removed for vm in parked)


def test_empty_warm_pool_claim_is_a_miss(monkeypatch):
    import vmon.pool as pool_mod

    class FakeVM:
        @classmethod
        def fork_snapshot(cls, template, agent=True):
            raise AssertionError("size=0 pool should not spawn")

    monkeypatch.setattr(pool_mod, "MicroVM", FakeVM)
    pool = pool_mod.WarmPool(template="x", size=0, ping_timeout=0.1)
    try:
        assert pool.claim() is None
        assert pool.stats()["misses"] == 1
        assert pool.stats()["ready_count"] == 0
    finally:
        pool.shutdown()
