#![allow(non_camel_case_types)]
use std::convert::From;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::os::raw::{c_char, c_int, c_uint, c_void};

pub type in_addr_t = u32;

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct in_addr {
    pub s_addr: in_addr_t,
}

impl From<Ipv4Addr> for in_addr {
    fn from(item: Ipv4Addr) -> Self {
        in_addr {
            s_addr: u32::to_be(item.into()),
        }
    }
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(target_os = "windows", repr(align(2)))]
#[cfg_attr(not(target_os = "windows"), repr(align(4)))]
#[repr(C)]
pub struct in6_addr {
    pub s6_addr: [u8; 16],
}

impl From<Ipv6Addr> for in6_addr {
    fn from(item: Ipv6Addr) -> Self {
        in6_addr {
            s6_addr: item.octets(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct sockaddr_in {
    pub sin_family: u16,
    pub sin_port: u16,
    pub sin_addr: in_addr,
    pub sin_zero: [u8; 8],
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct sockaddr_in6 {
    pub sin6_family: u16,
    pub sin6_port: u16,
    pub sin6_flowinfo: u32,
    pub sin6_addr: in6_addr,
    pub sin6_scope_id: u32,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Slirp {
    _unused: [u8; 0],
}

pub const SLIRP_POLL_IN: c_int = 1;
pub const SLIRP_POLL_OUT: c_int = 2;
pub const SLIRP_POLL_PRI: c_int = 4;
pub const SLIRP_POLL_ERR: c_int = 8;
pub const SLIRP_POLL_HUP: c_int = 16;

pub type SlirpReadCb = ::std::option::Option<
    unsafe extern "C" fn(buf: *mut c_void, len: usize, opaque: *mut c_void) -> isize,
>;
pub type SlirpWriteCb = ::std::option::Option<
    unsafe extern "C" fn(buf: *const c_void, len: usize, opaque: *mut c_void) -> isize,
>;
pub type SlirpTimerCb = ::std::option::Option<unsafe extern "C" fn(opaque: *mut c_void)>;
pub type SlirpAddPollCb = ::std::option::Option<
    unsafe extern "C" fn(fd: c_int, events: c_int, opaque: *mut c_void) -> c_int,
>;
pub type SlirpGetREventsCb =
    ::std::option::Option<unsafe extern "C" fn(idx: c_int, opaque: *mut c_void) -> c_int>;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct SlirpCb {
    pub send_packet: SlirpWriteCb,
    pub guest_error:
        ::std::option::Option<unsafe extern "C" fn(msg: *const c_char, opaque: *mut c_void)>,
    pub clock_get_ns: ::std::option::Option<unsafe extern "C" fn(opaque: *mut c_void) -> i64>,
    pub timer_new: ::std::option::Option<
        unsafe extern "C" fn(
            cb: SlirpTimerCb,
            cb_opaque: *mut c_void,
            opaque: *mut c_void,
        ) -> *mut c_void,
    >,
    pub timer_free:
        ::std::option::Option<unsafe extern "C" fn(timer: *mut c_void, opaque: *mut c_void)>,
    pub timer_mod: ::std::option::Option<
        unsafe extern "C" fn(timer: *mut c_void, expire_time: i64, opaque: *mut c_void),
    >,
    pub register_poll_fd:
        ::std::option::Option<unsafe extern "C" fn(fd: c_int, opaque: *mut c_void)>,
    pub unregister_poll_fd:
        ::std::option::Option<unsafe extern "C" fn(fd: c_int, opaque: *mut c_void)>,
    pub notify: ::std::option::Option<unsafe extern "C" fn(opaque: *mut c_void)>,
}

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct SlirpConfig {
    pub version: c_uint,
    pub restricted: c_int,
    pub in_enabled: bool,
    pub vnetwork: in_addr,
    pub vnetmask: in_addr,
    pub vhost: in_addr,
    pub in6_enabled: bool,
    pub vprefix_addr6: in6_addr,
    pub vprefix_len: u8,
    pub vhost6: in6_addr,
    pub vhostname: *const c_char,
    pub tftp_server_name: *const c_char,
    pub tftp_path: *const c_char,
    pub bootfile: *const c_char,
    pub vdhcp_start: in_addr,
    pub vnameserver: in_addr,
    pub vnameserver6: in6_addr,
    pub vdnssearch: *mut *const c_char,
    pub vdomainname: *const c_char,
    pub if_mtu: usize,
    pub if_mru: usize,
    pub disable_host_loopback: bool,
    pub enable_emu: bool,
    pub outbound_addr: *const sockaddr_in,
    pub outbound_addr6: *const sockaddr_in6,
    pub disable_dns: bool,
    pub disable_dhcp: bool,
}

extern "C" {
    pub fn slirp_init(
        restricted: c_int,
        in_enabled: bool,
        vnetwork: in_addr,
        vnetmask: in_addr,
        vhost: in_addr,
        in6_enabled: bool,
        vprefix_addr6: in6_addr,
        vprefix_len: u8,
        vhost6: in6_addr,
        vhostname: *const c_char,
        tftp_server_name: *const c_char,
        tftp_path: *const c_char,
        bootfile: *const c_char,
        vdhcp_start: in_addr,
        vnameserver: in_addr,
        vnameserver6: in6_addr,
        vdnssearch: *mut *const c_char,
        vdomainname: *const c_char,
        callbacks: *const SlirpCb,
        opaque: *mut c_void,
    ) -> *mut Slirp;

    pub fn slirp_new(
        config: *const SlirpConfig,
        callbacks: *const SlirpCb,
        opaque: *mut c_void,
    ) -> *mut Slirp;

    pub fn slirp_cleanup(slirp: *mut Slirp);

    pub fn slirp_pollfds_fill(
        slirp: *mut Slirp,
        timeout: *mut u32,
        add_poll: SlirpAddPollCb,
        opaque: *mut c_void,
    );

    pub fn slirp_pollfds_poll(
        slirp: *mut Slirp,
        select_error: c_int,
        get_revents: SlirpGetREventsCb,
        opaque: *mut c_void,
    );

    pub fn slirp_input(slirp: *mut Slirp, pkt: *const u8, pkt_len: c_int);

    pub fn slirp_add_hostfwd(
        slirp: *mut Slirp,
        is_udp: c_int,
        host_addr: in_addr,
        host_port: c_int,
        guest_addr: in_addr,
        guest_port: c_int,
    ) -> c_int;

    pub fn slirp_remove_hostfwd(
        slirp: *mut Slirp,
        is_udp: c_int,
        host_addr: in_addr,
        host_port: c_int,
    ) -> c_int;

    pub fn slirp_add_exec(
        slirp: *mut Slirp,
        cmdline: *const c_char,
        guest_addr: *mut in_addr,
        guest_port: c_int,
    ) -> c_int;

    pub fn slirp_add_guestfwd(
        slirp: *mut Slirp,
        write_cb: SlirpWriteCb,
        opaque: *mut c_void,
        guest_addr: *mut in_addr,
        guest_port: c_int,
    ) -> c_int;

    pub fn slirp_connection_info(slirp: *mut Slirp) -> *mut c_char;

    pub fn slirp_socket_recv(
        slirp: *mut Slirp,
        guest_addr: in_addr,
        guest_port: c_int,
        buf: *const u8,
        size: c_int,
    );

    pub fn slirp_socket_can_recv(
        slirp: *mut Slirp,
        guest_addr: in_addr,
        guest_port: c_int,
    ) -> usize;

    pub fn slirp_version_string() -> *const c_char;

    pub fn slirp_state_version() -> c_int;

    pub fn slirp_state_save(s: *mut Slirp, write_cb: SlirpWriteCb, opaque: *mut c_void);

    pub fn slirp_state_load(
        s: *mut Slirp,
        version_id: c_int,
        read_cb: SlirpReadCb,
        opaque: *mut c_void,
    ) -> c_int;
}
