use super::binds_overlap;

#[test]
fn rejects_shared_udp_sockets_but_not_other_ports() {
    for (left, right, expected) in [
        ("127.0.0.1:9444", "127.0.0.1:9444", true),
        ("127.0.0.1:9444", "0.0.0.0:9444", true),
        ("127.0.0.1:9444", "[::]:9444", true),
        ("127.0.0.1:9444", "[::ffff:127.0.0.1]:9444", true),
        ("127.0.0.1:9444", "127.0.0.1:443", false),
        ("192.0.2.1:9444", "192.0.2.2:9444", false),
    ] {
        let left = left.parse().unwrap();
        let right = right.parse().unwrap();
        assert_eq!(binds_overlap(left, right), expected);
        assert_eq!(binds_overlap(right, left), expected);
    }
}
