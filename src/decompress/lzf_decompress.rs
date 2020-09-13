use std::io;

pub fn lzf_decompress(buf_compressed: &[u8], buf_decompressed: &mut [u8]) -> io::Result<usize> {
    let mut in_pos = 0;
    let mut out_pos = 0;

    while in_pos < buf_compressed.len() {
        let ctrl = buf_compressed[in_pos] as usize;
        in_pos += 1;

        if ctrl < (1 << 5) {
            // literal run
            let run_len = ctrl + 1;

            buf_decompressed[out_pos..(out_pos + run_len)]
                .copy_from_slice(&buf_compressed[in_pos..(in_pos + run_len)]);

            in_pos += run_len;
            out_pos += run_len;
        } else {
            // back reference
            let run_len = match ctrl >> 5 {
                7 => {
                    // long back reference
                    let run_len_raw = buf_compressed[in_pos] as usize;
                    in_pos += 1;
                    run_len_raw + 9
                }
                run_len_raw => run_len_raw + 2,
            };

            let ref_offset_msb = (ctrl & 0x1F) << 8;
            let ref_offset_lsb = buf_compressed[in_pos] as usize;
            in_pos += 1;
            let ref_offset = ref_offset_msb + ref_offset_lsb + 1;
            let ref_pos = out_pos - ref_offset;

            if ref_pos + run_len <= out_pos {
                // non-overlapping
                let (src, dst) = buf_decompressed.split_at_mut(out_pos);
                dst[..run_len].copy_from_slice(&src[ref_pos..ref_pos + run_len]);
                out_pos += run_len;
            } else {
                // overlapping
                let mut ref_pos = ref_pos;
                for _ in 0..run_len {
                    buf_decompressed[out_pos] = buf_decompressed[ref_pos];
                    out_pos += 1;
                    ref_pos += 1;
                }
            }
        }
    }

    Ok(out_pos)
}
