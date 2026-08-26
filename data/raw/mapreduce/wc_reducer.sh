#!/bin/bash
# REDUCER — WordCount (bản awk)
# stdin ĐÃ SẮP XẾP theo khóa: mọi dòng cùng một từ nằm liền nhau.
# Nhờ vậy chỉ cần một biến đếm, không cần mảng chứa toàn bộ từ vựng.
awk -F'\t' '
$1 != khoa { if (khoa != "") print khoa "\t" tong; khoa = $1; tong = 0 }
           { tong += $2 }
END        { if (khoa != "") print khoa "\t" tong }
'
