#![allow(clippy::integer_arithmetic)]
use {
    serde::{
        de::{Error as _, SeqAccess, Visitor},
        ser::SerializeTuple,
        Deserializer, Serializer,
    },
    std::{fmt, marker::PhantomData},
};

pub trait VarInt: Sized {
    fn visit_seq<'de, A>(seq: A) -> Result<Self, A::Error>
    where
        A: SeqAccess<'de>;

    fn serialize<S>(self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer;
}

struct VarIntVisitor<T> {
    phantom: PhantomData<T>,
}

impl<'de, T> Visitor<'de> for VarIntVisitor<T>
where
    T: VarInt,
{
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a VarInt")
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        T::visit_seq(seq)
    }
}

pub fn serialize<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Copy + VarInt,
    S: Serializer,
{
    (*value).serialize(serializer)
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: VarInt,
{
    deserializer.deserialize_tuple(
        (std::mem::size_of::<T>() * 8 + 6) / 7,
        VarIntVisitor {
            phantom: PhantomData::default(),
        },
    )
}

macro_rules! impl_var_int {
    ($type:ty) => {
        impl VarInt for $type {
            fn visit_seq<'de, A>(mut seq: A) -> Result<Self, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut out = 0;
                let mut shift = 0u32;
                while shift < <$type>::BITS {
                    let byte = match seq.next_element::<u8>()? {
                        None => return Err(A::Error::custom("Invalid sequence")),
                        Some(byte) => byte,
                    };
                    out |= ((byte & 0x7F) as Self) << shift;
                    if byte & 0x80 == 0 {
                        if (out >> shift) as u8 != byte {
                            return Err(A::Error::custom("Invalid value"));
                        }
                        return Ok(out);
                    }
                    shift += 7;
                }
                Err(A::Error::custom("Invalid value"))
            }

            fn serialize<S>(mut self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let bits = <$type>::BITS - self.leading_zeros();
                let num_bytes = ((bits + 6) / 7).max(1) as usize;
                let mut seq = serializer.serialize_tuple(num_bytes)?;
                while self >= 0x80 {
                    let byte = ((self & 0x7F) | 0x80) as u8;
                    seq.serialize_element(&byte)?;
                    self >>= 7;
                }
                seq.serialize_element(&(self as u8))?;
                seq.end()
            }
        }
    };
}

impl_var_int!(u32);
impl_var_int!(u64);

#[cfg(test)]
mod tests {
    use rand::Rng;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct Dummy {
        #[serde(with = "super")]
        a: u32,
        b: u64,
        #[serde(with = "super")]
        c: u64,
        d: u32,
    }

    #[test]
    fn test_serde_varint() {
        assert_eq!((std::mem::size_of::<u32>() * 8 + 6) / 7, 5);
        assert_eq!((std::mem::size_of::<u64>() * 8 + 6) / 7, 10);
        let dummy = Dummy {
            a: 698,
            b: 370,
            c: 146,
            d: 796,
        };
        let bytes = bincode::serialize(&dummy).unwrap();
        assert_eq!(bytes.len(), 16);
        let other: Dummy = bincode::deserialize(&bytes).unwrap();
        assert_eq!(other, dummy);
    }

    #[test]
    fn test_serde_varint_zero() {
        let dummy = Dummy {
            a: 0,
            b: 0,
            c: 0,
            d: 0,
        };
        let bytes = bincode::serialize(&dummy).unwrap();
        assert_eq!(bytes.len(), 14);
        let other: Dummy = bincode::deserialize(&bytes).unwrap();
        assert_eq!(other, dummy);
    }

    #[test]
    fn test_serde_varint_max() {
        let dummy = Dummy {
            a: u32::MAX,
            b: u64::MAX,
            c: u64::MAX,
            d: u32::MAX,
        };
        let bytes = bincode::serialize(&dummy).unwrap();
        assert_eq!(bytes.len(), 27);
        let other: Dummy = bincode::deserialize(&bytes).unwrap();
        assert_eq!(other, dummy);
    }

    #[test]
    fn test_serde_varint_rand() {
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let dummy = Dummy {
                a: rng.gen::<u32>() >> rng.gen_range(0, u32::BITS),
                b: rng.gen::<u64>() >> rng.gen_range(0, u64::BITS),
                c: rng.gen::<u64>() >> rng.gen_range(0, u64::BITS),
                d: rng.gen::<u32>() >> rng.gen_range(0, u32::BITS),
            };
            let bytes = bincode::serialize(&dummy).unwrap();
            let other: Dummy = bincode::deserialize(&bytes).unwrap();
            assert_eq!(other, dummy);
        }
    }
}
