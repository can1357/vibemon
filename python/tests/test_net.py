from vmon.net import SandboxNetwork


def test_guest_config_exposes_only_canonical_keys():
    config = {
        "tap": "vmon0",
        "guest_ip": "10.0.0.2",
        "host_ip": "10.0.0.1",
        "prefix": 30,
        "dns": ["1.1.1.1"],
    }
    network = SandboxNetwork("sb", config, tap=None, tunnels=None, egress_allow=None)
    assert network.guest_config == {
        "tap": "vmon0",
        "guest_ip": "10.0.0.2",
        "host_ip": "10.0.0.1",
        "prefix": 30,
        "dns": ["1.1.1.1"],
    }
