#!/usr/bin/env python3
"""REDUCER — Doanh thu theo ngành hàng.

Ra:  category  TAB  so_giao_dich  TAB  doanh_thu
"""
import sys

khoa_hien_tai = None
dem = 0
tong = 0

for dong in sys.stdin:
    khoa, _, gia_tri = dong.rstrip("\n").partition("\t")
    if khoa != khoa_hien_tai:
        if khoa_hien_tai is not None:
            print(f"{khoa_hien_tai}\t{dem}\t{tong}")
        khoa_hien_tai, dem, tong = khoa, 0, 0
    dem += 1
    tong += int(gia_tri)

if khoa_hien_tai is not None:
    print(f"{khoa_hien_tai}\t{dem}\t{tong}")
