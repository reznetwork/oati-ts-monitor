import unittest

from http_header_text import decode_http_header_text, encode_http_header_text


class HttpHeaderTextTests(unittest.TestCase):
    def test_ascii_passes_through(self):
        self.assertEqual(encode_http_header_text("segway_villain"), "segway_villain")
        self.assertEqual(decode_http_header_text("segway_villain"), "segway_villain")

    def test_unicode_round_trip(self):
        value = "Багги Segway Villain SSV"
        encoded = encode_http_header_text(value)
        self.assertTrue(encoded.startswith("utf-8b64:"))
        encoded.encode("latin-1")
        self.assertEqual(decode_http_header_text(encoded), value)


if __name__ == "__main__":
    unittest.main()
