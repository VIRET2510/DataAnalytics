#!/bin/bash
# MAPPER — Doanh thu theo ngành hàng (bản awk)
# Cột: 1 transaction_id | 5 category | 7 quantity | 8 unit_price
# stdout: "category <TAB> quantity*unit_price"
awk -F',' '$1 != "transaction_id" { print $5 "\t" $7 * $8 }'
