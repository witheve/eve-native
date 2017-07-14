// |T|DDDDDDD|RRRRRRR|SMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM|
// T: Type Extension Flag (0 For Typed Math numbers)
// D: Domain [0, 127]
// R: Range [-64, 63]
// S: mantissa Sign bit
// M: Mantissa [-2^48, 2^48 - 1]

pub type Tagged = u64;

pub trait ToTagged {
    fn to_tagged(&self) -> u64;
}

pub trait FromTagged<T> {
    fn get_value(self) -> T;
}

impl ToTagged for u32 {
    fn to_tagged(&self) -> u64 {
        let result:u64 = (*self).into();
        result | (1 << 63)
    }
}

pub trait TaggedMath {
    fn is_number(self) -> bool;
    fn is_other(self) -> bool;
}

impl TaggedMath for Tagged {
    fn is_number(self) -> bool {
        self & (1 << 63) != 0
    }

    fn is_other(self) -> bool {
        self & (1 << 63) == 0
    }
}

#[test]
fn numerics_base() {
    let x:u32 = 10;
    println!("{:b}", x.to_tagged());
    assert!(x.to_tagged().is_number());
    assert!(!x.to_tagged().is_other());
}
