#!/usr/bin/env python3
"""REDUCER — WordCount.

Hadoop bảo đảm: các dòng vào stdin ĐÃ ĐƯỢC SẮP XẾP theo khóa, và mọi dòng
cùng khóa đi liền nhau. Nhờ vậy reducer chỉ cần một biến đếm, không cần
dictionary — đó là lý do reducer chạy được trên luồng dữ liệu lớn hơn RAM.
"""
import sys

khoa_hien_tai = None
tong = 0

for dong in sys.stdin:
    khoa, _, gia_tri = dong.rstrip("\n").partition("\t")
    if khoa != khoa_hien_tai:
        if khoa_hien_tai is not None:
            print(f"{khoa_hien_tai}\t{tong}")
        khoa_hien_tai, tong = khoa, 0
    tong += int(gia_tri)

if khoa_hien_tai is not None:
    print(f"{khoa_hien_tai}\t{tong}")
