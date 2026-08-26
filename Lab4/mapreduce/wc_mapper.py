#!/usr/bin/env python3
"""MAPPER — WordCount.

Hợp đồng của Hadoop Streaming:
    đọc từng dòng ở stdin  ->  ghi ra stdout các dòng "khóa <TAB> giá trị"

Quy tắc chuẩn hóa (PHẢI GIỐNG HỆT bản awk, nếu không hai kết quả sẽ lệch):
    1. thay các dấu câu . , ; : ! ? " ( ) [ ] bằng khoảng trắng
    2. hạ chữ hoa — CHỈ hạ chữ hoa ASCII (A-Z), giữ nguyên Đ, Ổ, Ầ...
    3. tách theo khoảng trắng, bỏ chuỗi rỗng
"""
import sys

DAU_CAU = str.maketrans({c: " " for c in '.,;:!?"()[]'})
HA_ASCII = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

for dong in sys.stdin:
    for tu in dong.translate(DAU_CAU).translate(HA_ASCII).split():
        print(f"{tu}\t1")
