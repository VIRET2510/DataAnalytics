#!/bin/bash
# MAPPER — WordCount (bản awk, dùng cho cụm Hadoop không cài Python)
# stdin: từng dòng văn bản   ->   stdout: "tu <TAB> 1"
# Quy tắc chuẩn hóa phải TRÙNG KHỚP wc_mapper.py:
#   - thay dấu câu . , ; : ! ? " ( ) [ ] bằng khoảng trắng
#   - tolower() của mawk chỉ hạ chữ hoa ASCII, giữ nguyên Đ / Ổ / Ầ
awk '{
    gsub(/[.,;:!?"()\[\]]/, " ")
    n = split(tolower($0), w, /[ \t]+/)
    for (i = 1; i <= n; i++) if (w[i] != "") print w[i] "\t1"
}'
