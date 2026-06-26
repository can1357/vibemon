//! Intel MultiProcessor (MP) table generation.
//!
//! Without ACPI, this table is how the guest kernel discovers the CPUs, the
//! IOAPIC, and the ISA-IRQ -> IOAPIC-pin routing. Ported from Firecracker
//! (Apache-2.0); struct layouts follow the Linux `mpspec_def.h`.
//!
//! The floating pointer is written at [`EBDA_START`] (the last 1 KiB of the
//! legacy 640 KiB base RAM), where Linux's MP scan looks for it.

use std::mem::size_of;

use vm_memory::{Address, ByteValued, Bytes, GuestAddress, GuestMemory};

use crate::bail;
use crate::layout::{
    APIC_DEFAULT_PHYS_BASE, EBDA_START, IO_APIC_DEFAULT_PHYS_BASE, NUM_IOAPIC_PINS,
};
use crate::memory::GuestMemoryMmap;
use crate::result::Result;

// MP configuration entry types.
const MP_PROCESSOR: u8 = 0;
const MP_BUS: u8 = 1;
const MP_IOAPIC: u8 = 2;
const MP_INTSRC: u8 = 3;
const MP_LINTSRC: u8 = 4;

// Interrupt source types.
const MP_INT: u8 = 0;
const MP_NMI: u8 = 1;
const MP_EXTINT: u8 = 3;

const CPU_ENABLED: u8 = 1;
const CPU_BOOTPROCESSOR: u8 = 2;
const MPC_APIC_USABLE: u8 = 0x01;

const APIC_VERSION: u8 = 0x14;
const CPU_STEPPING: u32 = 0x600;
const CPU_FEATURE_APIC: u32 = 0x200;
const CPU_FEATURE_FPU: u32 = 0x001;
const MPC_SPEC: u8 = 4;

const MAX_SUPPORTED_CPUS: u8 = 254;

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpfIntel {
    signature: [u8; 4],
    physptr: u32,
    length: u8,
    specification: u8,
    checksum: u8,
    feature1: u8,
    feature2: u8,
    feature3: u8,
    feature4: u8,
    feature5: u8,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcTable {
    signature: [u8; 4],
    length: u16,
    spec: u8,
    checksum: u8,
    oem: [u8; 8],
    productid: [u8; 12],
    oemptr: u32,
    oemsize: u16,
    oemcount: u16,
    lapic: u32,
    reserved: u32,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcCpu {
    type_: u8,
    apicid: u8,
    apicver: u8,
    cpuflag: u8,
    cpufeature: u32,
    featureflag: u32,
    reserved: [u32; 2],
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcBus {
    type_: u8,
    busid: u8,
    bustype: [u8; 6],
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcIoapic {
    type_: u8,
    apicid: u8,
    apicver: u8,
    flags: u8,
    apicaddr: u32,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcIntsrc {
    type_: u8,
    irqtype: u8,
    irqflag: u16,
    srcbus: u8,
    srcbusirq: u8,
    dstapic: u8,
    dstirq: u8,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Default)]
struct MpcLintsrc {
    type_: u8,
    irqtype: u8,
    irqflag: u16,
    srcbusid: u8,
    srcbusirq: u8,
    destapic: u8,
    destapiclint: u8,
}

// SAFETY: all of these are `#[repr(C, packed)]` plain-old-data structs.
unsafe impl ByteValued for MpfIntel {}
unsafe impl ByteValued for MpcTable {}
unsafe impl ByteValued for MpcCpu {}
unsafe impl ByteValued for MpcBus {}
unsafe impl ByteValued for MpcIoapic {}
unsafe impl ByteValued for MpcIntsrc {}
unsafe impl ByteValued for MpcLintsrc {}

fn compute_checksum<T: ByteValued>(v: &T) -> u8 {
    v.as_slice().iter().fold(0u8, |acc, b| acc.wrapping_add(*b))
}

fn mpf_intel_checksum(v: &MpfIntel) -> u8 {
    let checksum = compute_checksum(v).wrapping_sub(v.checksum);
    (!checksum).wrapping_add(1)
}

fn mp_size(num_cpus: u8) -> usize {
    size_of::<MpfIntel>()
        + size_of::<MpcTable>()
        + size_of::<MpcCpu>() * num_cpus as usize
        + size_of::<MpcBus>()
        + size_of::<MpcIoapic>()
        + size_of::<MpcIntsrc>() * NUM_IOAPIC_PINS as usize
        + size_of::<MpcLintsrc>() * 2
}

/// Write the MP table describing `num_cpus` processors into guest memory.
pub fn setup_mptable(mem: &GuestMemoryMmap, num_cpus: u8) -> Result<()> {
    if num_cpus > MAX_SUPPORTED_CPUS {
        bail!("{num_cpus} vCPUs exceeds MP table maximum of {MAX_SUPPORTED_CPUS}");
    }

    let total = mp_size(num_cpus);
    let mut base = GuestAddress(EBDA_START);
    let ioapicid: u8 = num_cpus + 1;

    // Bound-check, then zero the whole region.
    let end = base
        .checked_add((total - 1) as u64)
        .ok_or("MP table address overflow")?;
    if !mem.address_in_range(end) {
        bail!("not enough guest memory for MP table");
    }
    mem.write_slice(&vec![0u8; total], base)?;

    let mut num_entries: u16 = 0;
    let mut checksum: u8 = 0;

    // MP floating pointer.
    {
        let size = size_of::<MpfIntel>() as u64;
        let mut mpf = MpfIntel {
            signature: *b"_MP_",
            physptr: u32::try_from(base.raw_value() + size).unwrap(),
            length: 1,
            specification: 4,
            ..Default::default()
        };
        mpf.checksum = mpf_intel_checksum(&mpf);
        mem.write_obj(mpf, base)?;
        base = base.unchecked_add(size);
        num_entries += 1;
    }

    // Reserve space for the configuration header (filled in last).
    let table_base = base;
    base = base.unchecked_add(size_of::<MpcTable>() as u64);

    // One processor entry per vCPU.
    for cpu_id in 0..num_cpus {
        let mpc_cpu = MpcCpu {
            type_: MP_PROCESSOR,
            apicid: cpu_id,
            apicver: APIC_VERSION,
            cpuflag: CPU_ENABLED | if cpu_id == 0 { CPU_BOOTPROCESSOR } else { 0 },
            cpufeature: CPU_STEPPING,
            featureflag: CPU_FEATURE_APIC | CPU_FEATURE_FPU,
            ..Default::default()
        };
        mem.write_obj(mpc_cpu, base)?;
        base = base.unchecked_add(size_of::<MpcCpu>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&mpc_cpu));
        num_entries += 1;
    }

    // One ISA bus.
    {
        let mpc_bus = MpcBus {
            type_: MP_BUS,
            busid: 0,
            bustype: *b"ISA   ",
        };
        mem.write_obj(mpc_bus, base)?;
        base = base.unchecked_add(size_of::<MpcBus>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&mpc_bus));
        num_entries += 1;
    }

    // One IOAPIC.
    {
        let mpc_ioapic = MpcIoapic {
            type_: MP_IOAPIC,
            apicid: ioapicid,
            apicver: APIC_VERSION,
            flags: MPC_APIC_USABLE,
            apicaddr: IO_APIC_DEFAULT_PHYS_BASE,
        };
        mem.write_obj(mpc_ioapic, base)?;
        base = base.unchecked_add(size_of::<MpcIoapic>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&mpc_ioapic));
        num_entries += 1;
    }

    // Identity-map every IOAPIC pin to the matching ISA IRQ.
    for i in 0..u8::try_from(NUM_IOAPIC_PINS).unwrap() {
        let mpc_intsrc = MpcIntsrc {
            type_: MP_INTSRC,
            irqtype: MP_INT,
            irqflag: 0,
            srcbus: 0,
            srcbusirq: i,
            dstapic: ioapicid,
            dstirq: i,
        };
        mem.write_obj(mpc_intsrc, base)?;
        base = base.unchecked_add(size_of::<MpcIntsrc>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&mpc_intsrc));
        num_entries += 1;
    }

    // Local interrupt sources: ExtINT on LINT0, NMI on LINT1.
    {
        let lint_extint = MpcLintsrc {
            type_: MP_LINTSRC,
            irqtype: MP_EXTINT,
            irqflag: 0,
            srcbusid: 0,
            srcbusirq: 0,
            destapic: 0,
            destapiclint: 0,
        };
        mem.write_obj(lint_extint, base)?;
        base = base.unchecked_add(size_of::<MpcLintsrc>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&lint_extint));
        num_entries += 1;

        let lint_nmi = MpcLintsrc {
            type_: MP_LINTSRC,
            irqtype: MP_NMI,
            irqflag: 0,
            srcbusid: 0,
            srcbusirq: 0,
            destapic: 0xFF,
            destapiclint: 1,
        };
        mem.write_obj(lint_nmi, base)?;
        base = base.unchecked_add(size_of::<MpcLintsrc>() as u64);
        checksum = checksum.wrapping_add(compute_checksum(&lint_nmi));
        num_entries += 1;
    }

    // Now that the table length is known, write the configuration header.
    let table_end = base;
    let mut mpc_table = MpcTable {
        signature: *b"PCMP",
        length: u16::try_from(table_end.raw_value() - table_base.raw_value()).unwrap(),
        spec: MPC_SPEC,
        oem: *b"VMON    ",
        productid: *b"0000000000\0\0",
        oemcount: num_entries,
        lapic: APIC_DEFAULT_PHYS_BASE,
        ..Default::default()
    };
    checksum = checksum.wrapping_add(compute_checksum(&mpc_table));
    mpc_table.checksum = (!checksum).wrapping_add(1);
    mem.write_obj(mpc_table, table_base)?;

    Ok(())
}
