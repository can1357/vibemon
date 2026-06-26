//! KVM irqfd-backed interrupt lines.

use std::io;

use crate::os::EventFd;
use crate::result::Result;

/// A triggerable KVM interrupt source registered with irqfd.
pub struct IrqLine {
    evt: EventFd,
}

impl IrqLine {
    /// Wrap an already-registered eventfd as an interrupt line.
    pub(super) fn new(evt: EventFd) -> IrqLine {
        IrqLine { evt }
    }

    /// Pulse the guest interrupt line.
    pub fn trigger(&self) -> Result<()> {
        self.evt.write(1)?;
        Ok(())
    }
}

impl vm_superio::Trigger for IrqLine {
    type E = io::Error;

    fn trigger(&self) -> io::Result<()> {
        self.evt.write(1)
    }
}
