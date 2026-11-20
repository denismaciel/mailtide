//! Transfer decoding of one part's body as it arrives in chunks, with
//! exactly the output of the parser's whole-part decode
//! ([`mail_parser::Encoding::decode`]): the chunks a stream yields
//! concatenate to what reading the part whole returns.

use encodify::base64;
use mail_parser::Encoding;
use mailtide::gmail::{PartRef, TransferEncoding};

/// Decodes one part's encoded body, fed in pieces of any size.
#[derive(Debug)]
pub struct PartDecoder {
    state: State,
}

#[derive(Debug)]
enum State {
    /// No transfer encoding: bytes pass through.
    Identity,
    /// Base64: the decoder keeps the symbols of an incomplete quantum.
    Base64(base64::Decoder),
    /// Quoted-printable: input after the last point where decoding can be
    /// split without changing the output waits for more.
    QuotedPrintable(Vec<u8>),
    /// A body malformed for its encoding: kept whole and decoded at the end,
    /// best effort, exactly as the parser decodes it.
    Whole(Encoding, Vec<u8>),
}

impl PartDecoder {
    /// A decoder for the part `part` names.
    #[must_use]
    pub fn new(part: &PartRef) -> Self {
        let encoding = match part.encoding {
            TransferEncoding::Identity => Encoding::None,
            TransferEncoding::QuotedPrintable => Encoding::QuotedPrintable,
            TransferEncoding::Base64 => Encoding::Base64,
        };
        let state = match (part.irregular, part.encoding) {
            (true, _) => State::Whole(encoding, Vec::new()),
            (false, TransferEncoding::Identity) => State::Identity,
            (false, TransferEncoding::Base64) => State::Base64(base64::MIME.decoder()),
            (false, TransferEncoding::QuotedPrintable) => State::QuotedPrintable(Vec::new()),
        };
        Self { state }
    }

    /// Whether the part is decoded only once it has all arrived.
    #[must_use]
    pub fn is_whole(&self) -> bool {
        matches!(self.state, State::Whole(..))
    }

    /// Decode the next piece of the body, appending what can be decoded so
    /// far to `out`.
    pub fn feed(&mut self, encoded: &[u8], out: &mut Vec<u8>) {
        match &mut self.state {
            State::Identity => out.extend_from_slice(encoded),
            State::Base64(decoder) => {
                let mut rest = encoded;
                loop {
                    rest = rest
                        .get(decoder.decode_symbols(rest, out)..)
                        .unwrap_or_default();
                    match rest.split_first() {
                        None => break,
                        Some((b'=', tail)) => {
                            decoder.pad(out);
                            rest = tail;
                        }
                        Some((_, tail)) => rest = tail,
                    }
                }
            }
            State::QuotedPrintable(pending) => {
                pending.extend_from_slice(encoded);
                let split = qp_split(pending);
                if split > 0 {
                    Encoding::QuotedPrintable
                        .decode_append(pending.get(..split).unwrap_or_default(), out);
                    pending.drain(..split);
                }
            }
            State::Whole(_, pending) => pending.extend_from_slice(encoded),
        }
    }

    /// The body ended: append the rest of its decoding to `out`.
    pub fn finish(self, out: &mut Vec<u8>) {
        match self.state {
            State::Identity => {}
            State::Base64(decoder) => decoder.finish(out),
            State::QuotedPrintable(pending) => {
                Encoding::QuotedPrintable.decode_append(&pending, out);
            }
            State::Whole(encoding, pending) => encoding.decode_append(&pending, out),
        }
    }
}

/// The longest prefix of `pending` that quoted-printable decodes the same
/// way alone as followed by anything: it ends after a line feed, or after a
/// byte that ends no escape, soft break or trailing white space (neither
/// `=`, a blank nor a carriage return, and not the byte right after `=`).
fn qp_split(pending: &[u8]) -> usize {
    (1..=pending.len())
        .rev()
        .find(|&end| {
            let last = pending.get(end - 1).copied().unwrap_or(b'=');
            if last == b'\n' {
                return true;
            }
            let escape_open = end >= 2 && pending.get(end - 2) == Some(&b'=');
            !matches!(last, b'=' | b' ' | b'\t' | b'\r') && !escape_open
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(encoding: TransferEncoding, irregular: bool, body: &[u8]) -> PartRef {
        let length = u64::try_from(body.len()).unwrap_or(u64::MAX);
        PartRef {
            offset: 0,
            length,
            encoding,
            irregular,
            total: length,
        }
    }

    fn whole(part: &PartRef, body: &[u8]) -> Vec<u8> {
        let encoding = match part.encoding {
            TransferEncoding::Identity => Encoding::None,
            TransferEncoding::QuotedPrintable => Encoding::QuotedPrintable,
            TransferEncoding::Base64 => Encoding::Base64,
        };
        encoding.decode(body).into_owned()
    }

    fn chunked(part: &PartRef, pieces: &[&[u8]]) -> Vec<u8> {
        let mut decoder = PartDecoder::new(part);
        let mut out = Vec::new();
        for piece in pieces {
            decoder.feed(piece, &mut out);
        }
        decoder.finish(&mut out);
        out
    }

    /// splitmix64.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n.max(1)).unwrap_or(1)).unwrap_or(0)
        }
    }

    #[test]
    fn chunked_decoding_equals_the_whole_decode_at_every_split() {
        let alphabets: [(TransferEncoding, &[u8]); 3] = [
            (TransferEncoding::Identity, b"ab \t\r\n=\xc3\xa9"),
            (
                TransferEncoding::QuotedPrintable,
                b"ab \t\r\n=3D=E9=e9=\xc3\xa9",
            ),
            (TransferEncoding::Base64, b"QUJDRA==\r\n \tAZaz09+/-="),
        ];
        let mut rng = Rng(0x0dec_0de5_1234_5678);
        for _ in 0..20_000 {
            let (encoding, alphabet) = alphabets[rng.below(alphabets.len())];
            let body: Vec<u8> = (0..rng.below(40))
                .map(|_| alphabet[rng.below(alphabet.len())])
                .collect();
            let irregular = match encoding {
                TransferEncoding::Identity => false,
                TransferEncoding::QuotedPrintable => {
                    encodify::qp::BODY.strict().decoded_len(&body).is_err()
                }
                TransferEncoding::Base64 => base64::MIME.decoded_len(&body).is_err(),
            };
            let part = reference(encoding, irregular, &body);
            let expected = whole(&part, &body);
            for split in 0..=body.len() {
                let (a, b) = body.split_at(split);
                assert_eq!(
                    chunked(&part, &[a, b]),
                    expected,
                    "{encoding:?} {body:?} at {split}"
                );
            }
            let mut pieces = Vec::new();
            let mut rest = body.as_slice();
            while !rest.is_empty() {
                let (piece, tail) = rest.split_at(rng.below(rest.len()) + 1);
                pieces.push(piece);
                rest = tail;
            }
            assert_eq!(chunked(&part, &pieces), expected, "{encoding:?} {body:?}");
        }
    }

    #[test]
    fn a_soft_line_break_split_across_chunks_is_joined() {
        let part = reference(TransferEncoding::QuotedPrintable, false, b"caf=\r\n=E9");
        assert_eq!(
            chunked(&part, &[b"caf", b"=", b"\r", b"\n", b"=E9"]),
            b"caf\xe9"
        );
        assert_eq!(chunked(&part, &[b"caf=", b"\r", b"\n=E", b"9"]), b"caf\xe9");
    }

    #[test]
    fn base64_lines_that_are_not_multiples_of_four_carry_over() {
        let part = reference(
            TransferEncoding::Base64,
            false,
            b"SGVsbG8s\r\nIHdvcmxk\r\nIQ==",
        );
        assert_eq!(
            chunked(
                &part,
                &[b"SGV", b"sbG8s\r", b"\nIHdvc", b"mxk\r\nI", b"Q=", b"="]
            ),
            b"Hello, world!"
        );
        let mut decoder = PartDecoder::new(&part);
        let mut out = Vec::new();
        decoder.feed(b"SGVsb", &mut out);
        assert_eq!(out, b"Hel", "a whole quantum is decoded at once");
        decoder.feed(b"G8=", &mut out);
        assert_eq!(out, b"Hello", "padding completes the last quantum");
        decoder.finish(&mut out);
        assert_eq!(out, b"Hello");
    }

    #[test]
    fn an_irregular_part_is_decoded_whole() {
        let body = b"SGVs-bG8=";
        let part = reference(TransferEncoding::Base64, true, body);
        let decoder = PartDecoder::new(&part);
        assert!(decoder.is_whole());
        let mut decoder = decoder;
        let mut out = Vec::new();
        decoder.feed(b"SGVs", &mut out);
        decoder.feed(b"-bG8=", &mut out);
        assert!(out.is_empty(), "nothing before the end");
        decoder.finish(&mut out);
        assert_eq!(out, whole(&part, body));
        assert_eq!(out, b"Hello");
        let body = b"caf=E9 =ZZ";
        let part = reference(TransferEncoding::QuotedPrintable, true, body);
        assert_eq!(chunked(&part, &[b"caf=", b"E9 =Z", b"Z"]), b"caf\xe9 =ZZ");
    }
}
