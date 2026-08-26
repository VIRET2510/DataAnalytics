#!/usr/bin/env python3
"""MAPPER — Doanh thu theo ngành hàng.

Mỗi dòng giao dịch  ->  cặp  <category  TAB  quantity * unit_price>
Cột (đánh số từ 0): 4 = category, 6 = quantity, 7 = unit_price.
"""
import sys

for dong in sys.stdin:
    cot = dong.rstrip("\n").split(",")
    if len(cot) < 8 or cot[0] == "transaction_id":   # bỏ dòng tiêu đề và dòng hỏng
        continue
    try:
        print(f"{cot[4]}\t{int(cot[6]) * int(cot[7])}")
    except ValueError:
        continue          # dòng bẩn: bỏ qua, KHÔNG làm job chết
