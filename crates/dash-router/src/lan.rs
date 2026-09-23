//! The LAN boundary predicate (DESIGN.md; spec §6.1): the protocol only
//! runs between private-range peers. Loopback is accepted for local
//! development and tests.

use std::net::IpAddr;

pub fn is_lan(ip: IpAddr) -> bool {
    match ip {
        // is_private() is exactly DESIGN.md's three ranges:
        // 10/8, 172.16/12, 192.168/16.
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}

#[test]
fn lan_predicate_accepts_private_v4_and_loopback_only() {
    let yes = [
        "10.0.0.1",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.1.10",
        "127.0.0.1",
        "::1",
    ];
    let no = [
        "8.8.8.8",
        "172.32.0.1",
        "100.64.0.1",
        "2001:db8::1",
        "fe80::1",
    ];
    for ip in yes {
        assert!(is_lan(ip.parse().unwrap()), "{ip} should be LAN");
    }
    for ip in no {
        assert!(!is_lan(ip.parse().unwrap()), "{ip} should be dropped");
    }
}
