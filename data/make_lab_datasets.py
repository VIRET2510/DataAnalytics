"""Sinh hai bộ dữ liệu dùng cho Buổi 04 (Hadoop MapReduce) và Buổi 05 (Apache Spark).

Chạy:  python3 data/make_lab_datasets.py
Kết quả:
    data/raw/wordcount_corpus.txt   — kho văn bản tiếng Việt cho bài WordCount
    data/raw/transactions.csv       — nhật ký giao dịch bán lẻ (mặc định 200.000 dòng)

Toàn bộ dữ liệu sinh ra từ hạt giống cố định (random.seed) nên MỌI máy chạy đều
cho ra tệp giống hệt nhau, byte-by-byte. Nhờ đó mọi con số đáp án trong tài liệu
đều kiểm chứng được.
"""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

RAW = Path(__file__).resolve().parent / "raw"

# ---------------------------------------------------------------------------
# 1. KHO VĂN BẢN CHO WORDCOUNT
# ---------------------------------------------------------------------------
CORPUS = """\
Dữ liệu lớn không phải là một công nghệ mà là một tình huống.
Tình huống ấy xảy ra khi dữ liệu vượt quá sức chứa của một máy tính duy nhất.
Khi dữ liệu vượt quá bộ nhớ của một máy, mọi thói quen xử lý cũ đều phải viết lại.

Một nhà máy hiện đại gắn cảm biến lên từng máy công cụ.
Mỗi cảm biến gửi về một bản ghi sau mỗi giây.
Một máy sinh ra tám mươi sáu nghìn bản ghi mỗi ngày.
Năm mươi máy sinh ra hơn bốn triệu bản ghi mỗi ngày.
Một năm vận hành liên tục sinh ra hàng tỉ bản ghi cảm biến.
Không một máy tính cá nhân nào giữ nổi khối dữ liệu đó trong bộ nhớ.

Hệ thống tệp phân tán ra đời để giải bài toán sức chứa.
Hệ thống tệp phân tán cắt một tệp lớn thành nhiều khối nhỏ.
Mỗi khối được đặt lên một máy khác nhau trong cụm.
Mỗi khối còn được nhân bản sang vài máy nữa để chống mất dữ liệu.
Khi một máy hỏng, khối dữ liệu vẫn còn nguyên trên những máy còn lại.
Cụm càng nhiều máy thì sức chứa của cụm càng lớn.

Hadoop gọi hệ thống tệp phân tán của mình là HDFS.
HDFS có một nút quản lý tên gọi NameNode.
NameNode giữ siêu dữ liệu: tệp nào gồm những khối nào, khối nào nằm ở máy nào.
NameNode không giữ dữ liệu, nó chỉ giữ bản đồ của dữ liệu.
Các nút lưu trữ tên gọi DataNode mới thực sự giữ từng khối.
DataNode báo cáo tình trạng khối của mình về NameNode theo chu kỳ.
Mất một DataNode thì cụm vẫn chạy, mất NameNode thì cụm mất bản đồ.

MapReduce là cách tính toán đi kèm với hệ thống tệp phân tán.
MapReduce chia phép tính thành hai hàm rất nhỏ.
Hàm map đọc từng dòng dữ liệu và phát ra một cặp khóa giá trị.
Hàm reduce nhận toàn bộ giá trị của cùng một khóa và tổng hợp lại.
Giữa map và reduce có một giai đoạn tên là shuffle.
Shuffle gom mọi cặp cùng khóa về cùng một máy.
Shuffle là giai đoạn tốn kém nhất vì nó phải truyền dữ liệu qua mạng.
Lập trình viên viết map và viết reduce, khung Hadoop lo phần shuffle.

Nguyên tắc quan trọng nhất của MapReduce tên là data locality.
Data locality nghĩa là đưa phép tính đến chỗ dữ liệu.
Chương trình chỉ nặng vài trăm kilobyte còn dữ liệu nặng hàng trăm gigabyte.
Chuyển chương trình qua mạng rẻ hơn chuyển dữ liệu qua mạng rất nhiều lần.
Mỗi máy tính trên chính khối dữ liệu nằm sẵn trên đĩa của mình.
Nhiều máy cùng tính một lúc nên tổng thời gian giảm xuống.

YARN là bộ quản lý tài nguyên của cụm Hadoop.
YARN quyết định job nào được cấp bao nhiêu bộ nhớ và bao nhiêu lõi.
ResourceManager của YARN nhận yêu cầu và phân bổ tài nguyên.
NodeManager của YARN chạy các tiến trình con trên từng máy.
Một cụm có thể chạy nhiều job cùng lúc nhờ YARN đứng ra điều phối.

WordCount là bài toán đầu tiên của mọi khóa học MapReduce.
WordCount đếm số lần xuất hiện của mỗi từ trong một kho văn bản.
Hàm map của WordCount tách dòng thành từ và phát ra cặp từ và số một.
Hàm reduce của WordCount cộng dồn tất cả số một của cùng một từ.
Bài toán đơn giản nhưng nó chứa đủ cả bốn giai đoạn của mô hình.
Đổi kho văn bản từ một megabyte thành một terabyte thì mã nguồn không đổi.
Chỉ có số máy trong cụm là phải đổi.

Nhật ký giao dịch của một chuỗi bán lẻ cũng là dữ liệu lớn.
Mỗi lần khách trả tiền, hệ thống ghi một dòng vào nhật ký.
Dòng nhật ký gồm mã giao dịch, thời gian, cửa hàng, ngành hàng, sản phẩm.
Dòng nhật ký còn ghi số lượng, đơn giá và phương thức thanh toán.
Doanh thu của một dòng bằng số lượng nhân đơn giá.
Tổng doanh thu theo ngành hàng chính là một phép reduce theo khóa ngành hàng.
Tổng doanh thu theo cửa hàng chính là một phép reduce theo khóa cửa hàng.
Cùng một dữ liệu, đổi khóa thì đổi báo cáo.

MapReduce ghi kết quả trung gian xuống đĩa sau mỗi giai đoạn.
Ghi xuống đĩa giúp job hồi phục được khi một máy chết giữa chừng.
Ghi xuống đĩa cũng khiến job chậm đi rất nhiều lần.
Một thuật toán lặp hai mươi vòng phải ghi và đọc đĩa hai mươi lần.
Đó là điểm yếu lớn nhất của MapReduce.

Apache Spark sinh ra để sửa đúng điểm yếu đó.
Spark giữ dữ liệu trung gian trong bộ nhớ thay vì ghi xuống đĩa.
Spark gọi tập dữ liệu phân tán của mình là RDD.
RDD là bất biến, đã tạo ra thì không sửa được nữa.
Mỗi phép biến đổi trên RDD sinh ra một RDD mới.
Spark ghi nhớ chuỗi biến đổi ấy dưới dạng một đồ thị.
Đồ thị đó cho phép Spark dựng lại phần dữ liệu bị mất mà không cần ghi đĩa.

Spark chia phép toán thành hai loại là biến đổi và hành động.
Biến đổi thì lười, Spark chỉ ghi lại chứ chưa tính.
Hành động mới kích hoạt toàn bộ chuỗi biến đổi đã ghi.
Nhờ lười mà Spark nhìn thấy trọn kế hoạch trước khi chạy.
Nhìn thấy trọn kế hoạch thì tối ưu được, gộp được nhiều bước làm một.

DataFrame là lớp trừu tượng cao hơn RDD.
DataFrame có lược đồ, biết tên cột và kiểu dữ liệu của từng cột.
Biết lược đồ thì bộ tối ưu Catalyst viết lại được câu truy vấn cho nhanh hơn.
Cùng một phép đếm, DataFrame thường nhanh hơn RDD viết tay.
Người mới nên bắt đầu từ DataFrame và chỉ dùng RDD khi thật cần.

So sánh công bằng giữa hai công nghệ đòi hỏi cùng một dữ liệu và cùng một phép tính.
Trên dữ liệu nhỏ, chi phí khởi động lấn át thời gian tính toán thật.
Trên dữ liệu nhỏ, một chương trình một máy luôn thắng cả Hadoop lẫn Spark.
Trên dữ liệu lớn, Hadoop và Spark mới cho thấy giá trị của mình.
Chọn công cụ theo quy mô dữ liệu chứ không theo độ mới của công nghệ.

Người làm dữ liệu cần biết cả hai.
Hadoop dạy ta cách nghĩ theo khóa và giá trị.
Spark cho ta cách nghĩ ấy với tốc độ của bộ nhớ.
Hiểu MapReduce thì hiểu luôn vì sao Spark nhanh.
Không hiểu MapReduce thì Spark chỉ còn là một thư viện lạ.
"""


def viet_corpus(nhan_ban: int = 1) -> Path:
    RAW.mkdir(parents=True, exist_ok=True)
    duong_dan = RAW / "wordcount_corpus.txt"
    duong_dan.write_text(CORPUS * nhan_ban, encoding="utf-8")
    return duong_dan


# ---------------------------------------------------------------------------
# 2. NHẬT KÝ GIAO DỊCH BÁN LẺ
# ---------------------------------------------------------------------------
# (ngành hàng, [ (tên sản phẩm, đơn giá cơ sở) ... ], trọng số xuất hiện)
DANH_MUC = {
    "Điện tử": ([("Tai nghe không dây", 890_000), ("Chuột quang", 320_000),
                 ("Bàn phím cơ", 1_450_000), ("Ổ cứng SSD 512GB", 1_290_000),
                 ("Sạc dự phòng", 540_000)], 18),
    "Gia dụng": ([("Nồi cơm điện", 1_180_000), ("Ấm siêu tốc", 390_000),
                  ("Máy xay sinh tố", 760_000), ("Quạt điều hòa", 2_350_000),
                  ("Bộ dao nhà bếp", 450_000)], 16),
    "Thực phẩm": ([("Gạo ST25 5kg", 185_000), ("Dầu ăn 1L", 62_000),
                   ("Sữa tươi thùng", 340_000), ("Cà phê rang xay", 155_000),
                   ("Mì gói thùng", 128_000)], 30),
    "Thời trang": ([("Áo sơ mi", 420_000), ("Quần jeans", 680_000),
                    ("Giày thể thao", 1_250_000), ("Áo khoác gió", 890_000),
                    ("Túi xách", 1_540_000)], 14),
    "Sách": ([("Sách kỹ năng", 128_000), ("Sách thiếu nhi", 76_000),
              ("Giáo trình đại học", 210_000), ("Truyện tranh", 45_000),
              ("Từ điển", 320_000)], 12),
    "Mỹ phẩm": ([("Sữa rửa mặt", 245_000), ("Kem chống nắng", 385_000),
                 ("Son môi", 320_000), ("Nước hoa mini", 690_000),
                 ("Serum dưỡng da", 850_000)], 10),
}

CUA_HANG = [
    ("S01", "Hà Nội"), ("S02", "Hà Nội"), ("S03", "Hải Phòng"),
    ("S04", "Đà Nẵng"), ("S05", "Huế"), ("S06", "TP HCM"),
    ("S07", "TP HCM"), ("S08", "TP HCM"), ("S09", "Cần Thơ"),
    ("S10", "Bình Dương"),
]
# trọng số cửa hàng: ba cửa hàng TP HCM và hai cửa hàng Hà Nội đông khách hơn
TRONG_SO_CUA_HANG = [14, 11, 7, 9, 5, 16, 13, 10, 8, 7]

THANH_TOAN = ["TIEN_MAT", "THE", "VI_DIEN_TU", "CHUYEN_KHOAN"]
TRONG_SO_THANH_TOAN = [34, 30, 26, 10]

BAT_DAU = datetime(2025, 1, 1, 7, 0, 0)


def viet_transactions(so_dong: int = 200_000, seed: int = 42) -> Path:
    rng = random.Random(seed)
    RAW.mkdir(parents=True, exist_ok=True)
    duong_dan = RAW / "transactions.csv"

    ten_nganh = list(DANH_MUC)
    trong_so_nganh = [DANH_MUC[n][1] for n in ten_nganh]

    with duong_dan.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["transaction_id", "ts", "store_id", "city", "category",
                    "product", "quantity", "unit_price", "payment_method",
                    "customer_id"])
        moc = BAT_DAU
        for i in range(1, so_dong + 1):
            moc += timedelta(seconds=rng.randint(1, 140))
            nganh = rng.choices(ten_nganh, weights=trong_so_nganh, k=1)[0]
            san_pham, gia_goc = rng.choice(DANH_MUC[nganh][0])
            # dao động giá +/- 8% theo bội số 1000 đồng
            don_gia = int(gia_goc * rng.uniform(0.92, 1.08) / 1000) * 1000
            so_luong = rng.choices([1, 2, 3, 4, 5], weights=[55, 24, 12, 6, 3], k=1)[0]
            cua_hang, thanh_pho = rng.choices(CUA_HANG, weights=TRONG_SO_CUA_HANG, k=1)[0]
            w.writerow([
                f"T{i:07d}",
                moc.strftime("%Y-%m-%d %H:%M:%S"),
                cua_hang, thanh_pho, nganh, san_pham, so_luong, don_gia,
                rng.choices(THANH_TOAN, weights=TRONG_SO_THANH_TOAN, k=1)[0],
                f"C{rng.randint(1, 25_000):05d}",
            ])
    return duong_dan


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Sinh dữ liệu cho Buổi 04 và Buổi 05")
    ap.add_argument("--rows", type=int, default=200_000, help="số dòng giao dịch")
    ap.add_argument("--corpus-repeat", type=int, default=1, help="số lần nhân bản kho văn bản")
    a = ap.parse_args()

    p1 = viet_corpus(a.corpus_repeat)
    p2 = viet_transactions(a.rows)
    for p in (p1, p2):
        print(f"{p}  —  {p.stat().st_size/1024**2:.2f} MB")
