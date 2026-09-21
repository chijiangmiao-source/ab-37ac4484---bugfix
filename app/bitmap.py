"""Bit-packed received-chunk bitmap persisted as a SQLite BLOB."""

from __future__ import annotations


def new_bitmap(total: int) -> bytearray:
    return bytearray((total + 7) // 8)


def set_bit(bitmap, index: int) -> None:
    bitmap[index >> 3] |= 1 << (index & 7)


def is_set(bitmap, index: int) -> bool:
    return bool(bitmap[index >> 3] & (1 << (index & 7)))


def missing_indices(bitmap, total: int) -> list[int]:
    return [i for i in range(total) if not is_set(bitmap, i)]


def count_set(bitmap, total: int) -> int:
    return sum(1 for i in range(total) if is_set(bitmap, i))
