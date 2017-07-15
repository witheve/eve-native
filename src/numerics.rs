// |T|DDDDDDD|RRRRRRR|SMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM|
// T: Type Extension Flag (0 For Typed Math numbers)
// D: Domain [0, 127]
// R: Range [-64, 63]
// S: mantissa Sign bit
// M: Mantissa [-2^48, 2^48 - 1]

const EXTENSION_MASK:u64 = 1 << 63;
const MANTISSA_MASK:u64 = (((1 as u64) << 49) as u64 - 1); // 49 bits at the end
const META_MASK:u64 = ((1 << 15) as u64 - 1) << 49; // 15 1s at the front
const RANGE_MASK:u64 = ((1 << 7) as u64 - 1) << 49;
const DOMAIN_MASK:u64 = ((1 << 7) as u64 - 1) << 56;
const SHIFTED_RANGE_DOMAIN_MASK:u64 = ((1 << 7) as u64 - 1);
const SHIFTED_FILL:u64 = ((((1 as u64) << 57) as u64 - 1) << 7);
const SIGN_MASK:u64 = 1 << 48;

pub type Tagged = u64;

pub trait ToTagged {
    fn to_tagged(&self) -> u64;
}

pub trait FromTagged<T> {
    fn get_value(self) -> T;
}

impl ToTagged for u32 {
    #[inline(always)]
    fn to_tagged(&self) -> u64 {
        let result:u64 = (*self).into();
        result | (1 << 63)
    }
}

impl ToTagged for i32 {
    #[inline(always)]
    fn to_tagged(&self) -> u64 {
        let me = *self;
        if me.is_negative() {
            me as u64 & MANTISSA_MASK | EXTENSION_MASK
        } else {
            me as u64 | EXTENSION_MASK
        }
    }
}

impl ToTagged for u64 {
    #[inline(always)]
    fn to_tagged(&self) -> u64 {
        let me = *self;
        if me & META_MASK != 0 {
            let (mantissa, range) = overflow_handler(me);
            (mantissa as u64) & MANTISSA_MASK | shifted_range(range) |  EXTENSION_MASK
        } else {
            me & MANTISSA_MASK | EXTENSION_MASK
        }
    }
}

impl ToTagged for i64 {
    #[inline(always)]
    fn to_tagged(&self) -> u64 {
        let me = *self;
        if me.is_negative() {
            if (me as u64) & META_MASK != META_MASK {
                let (mantissa, range) = overflow_handler(me.abs() as u64);
                !(mantissa - 1) & MANTISSA_MASK | shifted_range(range) |  EXTENSION_MASK
            } else {
                (me as u64) & MANTISSA_MASK | EXTENSION_MASK
            }
        } else if (me as u64) & META_MASK != 0 {
            let (mantissa, range) = overflow_handler(me as u64);
            (mantissa as u64) & MANTISSA_MASK | shifted_range(range) |  EXTENSION_MASK
        } else {
            (me as u64) & MANTISSA_MASK | EXTENSION_MASK
        }
    }
}

#[inline(always)]
pub fn overflow_handler(me:u64) -> (u64, u64) {
    let hi = 64 - me.leading_zeros() - 48;
    let r = (2u64.pow(hi) as f64).log10().ceil() as u32;
    let result = me / 10u64.pow(r) as u64;
    (result, r as u64)
}

pub fn decrease_range(mantissa:i64, range_delta:u64) -> (i64, u64) {
    let hi = mantissa.leading_zeros();
    let thing = 1 << (hi + 1);
    let hi_10 = (thing as f64).log10().floor() as u64;
    if range_delta <= hi_10 {
        (mantissa * 10u64.pow(range_delta as u32) as i64, range_delta)
    } else {
        (mantissa * 10u64.pow(hi_10 as u32) as i64, range_delta)
    }
}

pub fn increase_range(mantissa:i64, range_delta:u64) -> (i64, bool) {
    let range = 10u64.pow(range_delta as u32) as i64;
    (mantissa / range, mantissa % range != 0)
}

#[inline(always)]
pub fn shifted_range(range:u64) -> u64 {
    range << 49
}

pub fn make_tagged(mantissa:u64, range:i64, domain:u64) -> Tagged {
    let value = mantissa.to_tagged();
    let cur_range = (value.range() + range) as u64;
    value & !RANGE_MASK | ((cur_range << 49) & RANGE_MASK) | (domain << 56)
}

pub trait TaggedMath {
    fn is_number(self) -> bool;
    fn is_other(self) -> bool;
    fn domain(self) -> u64;
    fn range(self) -> i64;
    fn mantissa(self) -> i64;
    fn is_negative(self) -> bool;
    fn add(self, Tagged) -> Tagged;
    fn sub(self, Tagged) -> Tagged;
    fn multiply(self, Tagged) -> Tagged;
}

impl TaggedMath for Tagged {
    #[inline(always)]
    fn is_number(self) -> bool {
        self & EXTENSION_MASK == EXTENSION_MASK
    }

    #[inline(always)]
    fn is_other(self) -> bool {
        self & EXTENSION_MASK == 0
    }

    #[inline(always)]
    fn domain(self) -> u64 {
        (self >> 49) & SHIFTED_RANGE_DOMAIN_MASK
    }

    #[inline(always)]
    fn range(self) -> i64 {
        let range = (self >> 49) & SHIFTED_RANGE_DOMAIN_MASK;
        if range & (1 << 7) != 0 {
            range as i64
        } else {
            (range | SHIFTED_FILL) as i64
        }
    }

    #[inline(always)]
    fn mantissa(self) -> i64 {
        if self & SIGN_MASK == SIGN_MASK {
            let a = self & MANTISSA_MASK;
            (a as i64) | (META_MASK as i64)
        } else {
            (self & MANTISSA_MASK) as i64
        }
    }

    #[inline(always)]
    fn is_negative(self) -> bool {
        (self & SIGN_MASK) == SIGN_MASK
    }

    #[inline(always)]
    fn add(self, other:Tagged) -> Tagged {
        let my_range = self & RANGE_MASK;
        let other_range = self & RANGE_MASK;
        if my_range == other_range {
            let added = self.mantissa() + other.mantissa();
            added.to_tagged()
        } else {
            let my_mant = self.mantissa();
            let other_mant = other.mantissa();
            let (a_range, b_range, a_mant, b_mant) = if my_range > other_range {
                (my_range, other_range, my_mant, other_mant)
            } else {
                (other_range, my_range, other_mant, my_mant)
            };
            let range_delta = (a_range - b_range) >> 49;
            let (neue, actual_delta) = decrease_range(a_mant, range_delta);
            if actual_delta == range_delta {
                let added = neue + b_mant;
                added.to_tagged()
            } else {
                let (b_neue, _) = increase_range(b_mant, actual_delta);
                let added = neue + b_neue;
                added.to_tagged()
            }
        }
    }

    fn sub(self, other:Tagged) -> Tagged {
        let result = self.mantissa() - other.mantissa();
        result.to_tagged()
    }

    fn multiply(self, other:Tagged) -> Tagged {
        let result = self.mantissa() * other.mantissa();
        result.to_tagged()
    }
}

#[test]
fn numerics_base() {
    let x:i64 = -1 * (2u64.pow(52) as i64);
    // let y:i64 = -1;
    println!("{:b}", make_tagged(1, 3, 1));
    println!("{:b}", x.to_tagged());
    println!("{}", x.to_tagged().mantissa());
    // println!("{:b}", y.to_tagged());
    // println!("{:b}", META_MASK);
    // println!("{}", x.to_tagged().add(y.to_tagged()).mantissa());
    // println!("{:b} {:b}", ((1 << 16) as u64 - 1) << 49, (2u64.pow(15) - 1) << 49);
    // assert!(x.to_tagged().is_number());
    // assert!(!x.to_tagged().is_other());
    // assert_eq!(x.to_tagged().mantissa(), x as i64);
    // assert_eq!(y.to_tagged().mantissa(), y as i64);
    // assert_eq!(y.to_tagged().add(x.to_tagged()).mantissa(), 9);
}

extern crate test;
use self::test::{Bencher};
#[bench]
fn bench_numerics_add(b:&mut Bencher) {
    let y:i32 = -1;
    // let xs = (0..10000).map(|x| x.to_tagged()).collect::<Vec<_>>();
    let y_tagged = y.to_tagged();
    println!(" YO {:b}", y_tagged);
    println!(" YO2 {:b}", ((-1 as i32) as i64));
    b.iter(|| {
        for x in (0..10000).map(|x| x.to_tagged()) {
            test::black_box(x.add(y_tagged));
        }
    });
}

#[bench]
fn bench_numerics_normal_add(b:&mut Bencher) {
    let y:i32 = -1;
    b.iter(|| {
        for x in 0..10000 {
            test::black_box(x + y);
        }
    });
}
