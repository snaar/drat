const PRIME32_1: u32 = 0x9E3779B1;
const PRIME32_2: u32 = 0x85EBCA77;
const PRIME32_3: u32 = 0xC2B2AE3D;
const PRIME32_4: u32 = 0x27D4EB2F;
const PRIME32_5: u32 = 0x165667B1;

pub struct XXHash32 {
    v1: u32,
    v2: u32,
    v3: u32,
    v4: u32,
    buf: Box<[u8]>,
    buf_used: usize,
    did_at_least_one_full_round: bool,
    total_len_mod_32_bit: u32,
}

impl XXHash32 {
    pub fn new(seed: u32) -> XXHash32 {
        XXHash32 {
            v1: seed.wrapping_add(PRIME32_1).wrapping_add(PRIME32_2),
            v2: seed.wrapping_add(PRIME32_2),
            v3: seed,
            v4: seed.wrapping_sub(PRIME32_1),
            buf: vec![0; 16].into_boxed_slice(),
            buf_used: 0,
            did_at_least_one_full_round: false,
            total_len_mod_32_bit: 0,
        }
    }

    pub fn update(&mut self, input: &[u8]) {
        let input_len = input.len();

        self.total_len_mod_32_bit = self.total_len_mod_32_bit.wrapping_add(input_len as u32);

        if input_len >= 16 || self.total_len_mod_32_bit >= 16 {
            self.did_at_least_one_full_round = true;
        }

        let mut remaining = self.buf_used + input_len;

        if remaining < 16 {
            self.buf[self.buf_used..remaining].copy_from_slice(input);
            self.buf_used += input_len;
            return;
        }

        let mut input_offset = 0;

        if self.buf_used != 0 {
            input_offset += 16 - self.buf_used;
            self.buf[self.buf_used..].copy_from_slice(&input[..input_offset]);

            self.v1 = Self::round(self.v1, Self::read32le(&self.buf, 0));
            self.v2 = Self::round(self.v2, Self::read32le(&self.buf, 4));
            self.v3 = Self::round(self.v3, Self::read32le(&self.buf, 8));
            self.v4 = Self::round(self.v4, Self::read32le(&self.buf, 12));

            remaining -= 16;
            self.buf_used = 0;
        }

        while remaining >= 16 {
            self.v1 = Self::round(self.v1, Self::read32le(input, input_offset));
            self.v2 = Self::round(self.v2, Self::read32le(input, input_offset + 4));
            self.v3 = Self::round(self.v3, Self::read32le(input, input_offset + 8));
            self.v4 = Self::round(self.v4, Self::read32le(input, input_offset + 12));

            input_offset += 16;
            remaining -= 16;
        }

        if remaining != 0 {
            self.buf[..remaining].copy_from_slice(&input[input_offset..]);
            self.buf_used = remaining;
        }
    }

    pub fn digest(&self) -> u32 {
        let mut hash = if self.did_at_least_one_full_round {
            let v1 = self.v1.rotate_left(1);
            let v2 = self.v2.rotate_left(7);
            let v3 = self.v3.rotate_left(12);
            let v4 = self.v4.rotate_left(18);
            v1.wrapping_add(v2).wrapping_add(v3).wrapping_add(v4)
        } else {
            self.v3.wrapping_add(PRIME32_5)
        };

        hash += self.total_len_mod_32_bit;

        let mut offset = 0;
        let mut remaining = self.buf_used;

        while remaining >= 4 {
            hash = hash.wrapping_add(Self::read32le(&self.buf, offset).wrapping_mul(PRIME32_3));
            hash = hash.rotate_left(17);
            hash = hash.wrapping_mul(PRIME32_4);

            offset += 4;
            remaining -= 4;
        }

        while remaining != 0 {
            hash = hash.wrapping_add((self.buf[offset] as u32).wrapping_mul(PRIME32_5));
            hash = hash.rotate_left(11);
            hash = hash.wrapping_mul(PRIME32_1);

            offset += 1;
            remaining -= 1;
        }

        Self::avalanche(hash)
    }

    fn read32le(input: &[u8], offset: usize) -> u32 {
        (input[offset] as u32)
            | ((input[offset + 1] as u32) << 8)
            | ((input[offset + 2] as u32) << 16)
            | ((input[offset + 3] as u32) << 24)
    }

    fn round(v: u32, input: u32) -> u32 {
        let v = v.wrapping_add(input.wrapping_mul(PRIME32_2));
        let v = v.rotate_left(13);
        v.wrapping_mul(PRIME32_1)
    }

    fn avalanche(hash: u32) -> u32 {
        let hash = hash ^ (hash >> 15);
        let hash = hash.wrapping_mul(PRIME32_2);
        let hash = hash ^ (hash >> 13);
        let hash = hash.wrapping_mul(PRIME32_3);
        hash ^ (hash >> 16)
    }
}
