import struct


def body_and_tail(data):
    length = len(data)
    nblocks = length // 16
    tail = length % 16
    if nblocks:
        # we use '<', specifying little-endian byte order for data bigger than
        # a byte so behavior is the same on little- and big-endian platforms
        return struct.unpack_from('<' + ('qq' * nblocks), data), struct.unpack_from('b' * tail, data, -tail), length
    else:
        return tuple(), struct.unpack_from('b' * tail, data, -tail), length


def rotl64(x, r):
    # note: not a general-purpose function because it leaves the high-order bits intact
    # suitable for this use case without wasting cycles
    mask = 2 ** r - 1
    rotated = (x << r) | ((x >> 64 - r) & mask)
    return rotated


def fmix(k):
    # masking off the 31s bits that would be leftover after >> 33 a 64-bit number
    k ^= (k >> 33) & 0x7fffffff
    k *= 0xff51afd7ed558ccd
    k ^= (k >> 33) & 0x7fffffff
    k *= 0xc4ceb9fe1a85ec53
    k ^= (k >> 33) & 0x7fffffff
    return k


INT64_MAX = int(2 ** 63 - 1)
INT64_MIN = -INT64_MAX - 1
INT64_OVF_OFFSET = INT64_MAX + 1
INT64_OVF_DIV = 2 * INT64_OVF_OFFSET


def truncate_int64(x):
    if not INT64_MIN <= x <= INT64_MAX:
        x = (x + INT64_OVF_OFFSET) % INT64_OVF_DIV - INT64_OVF_OFFSET
    return x


def _murmur3(data):

    h1 = h2 = 0

    c1 = -8663945395140668459  # 0x87c37b91114253d5
    c2 = 0x4cf5ad432745937f

    body, tail, total_len = body_and_tail(data)

    # body
    for i in range(0, len(body), 2):
        k1 = body[i]
        k2 = body[i + 1]

        k1 *= c1
        k1 = rotl64(k1, 31)
        k1 *= c2
        h1 ^= k1

        h1 = rotl64(h1, 27)
        h1 += h2
        h1 = h1 * 5 + 0x52dce729

        k2 *= c2
        k2 = rotl64(k2, 33)
        k2 *= c1
        h2 ^= k2

        h2 = rotl64(h2, 31)
        h2 += h1
        h2 = h2 * 5 + 0x38495ab5

    # tail
    k1 = k2 = 0
    len_tail = len(tail)
    if len_tail > 8:
        for i in range(len_tail - 1, 7, -1):
            k2 ^= tail[i] << (i - 8) * 8
        k2 *= c2
        k2 = rotl64(k2, 33)
        k2 *= c1
        h2 ^= k2

    if len_tail:
        for i in range(min(7, len_tail - 1), -1, -1):
            k1 ^= tail[i] << i * 8
        k1 *= c1
        k1 = rotl64(k1, 31)
        k1 *= c2
        h1 ^= k1

    # finalization
    h1 ^= total_len
    h2 ^= total_len

    h1 += h2
    h2 += h1

    h1 = fmix(h1)
    h2 = fmix(h2)

    h1 += h2

    return truncate_int64(h1)


try:
    from cassandra.cmurmur3 import murmur3
except ImportError:
    murmur3 = _murmur3
