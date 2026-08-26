#!/bin/bash
# REDUCER — Doanh thu theo ngành hàng (bản awk)
# stdout: "category <TAB> so_giao_dich <TAB> doanh_thu"
awk -F'\t' '
$1 != khoa { if (khoa != "") printf "%s\t%d\t%.0f\n", khoa, dem, tong; khoa = $1; dem = 0; tong = 0 }
           { dem += 1; tong += $2 }
END        { if (khoa != "") printf "%s\t%d\t%.0f\n", khoa, dem, tong }
'
