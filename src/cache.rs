use std::ops::Deref;

#[cfg_attr(any(target_arch = "aarch64", target_arch = "arm"), repr(align(128)))]
#[cfg_attr(not(any(target_arch = "aarch64", target_arch = "arm")), repr(align(64)))]
pub struct Padding<T>(pub T);

impl <T> Deref for Padding<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}