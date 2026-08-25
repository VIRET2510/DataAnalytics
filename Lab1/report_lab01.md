# BÁO CÁO TÓM TẮT LAB 01 — PHÂN TÍCH 5V BIG DATA

**Bộ dữ liệu:** AI4I 2020 Predictive Maintenance (`ai4i2020.csv`)
**Bài toán:** Bảo trì dự đoán (Predictive Maintenance) cho máy móc nhà máy

## 1. Volume (Khối lượng)
- Tệp mẫu: 10,000 dòng x 14 cột, chiếm 1.92 MB trong RAM (~201 bytes/dòng).
- Ngoại suy thực tế (500 máy/nhà máy, lấy mẫu 1 Hz): 43,200,000 dòng/ngày = 8.09 GB/ngày.
- Quy mô 20 nhà máy: 57.65 TB/năm -> vượt xa năng lực của một máy đơn.

## 2. Velocity (Tốc độ)
- Tệp mẫu: dữ liệu tĩnh, xử lý theo lô (batch).
- Hệ thống thật: 500 bản ghi/giây (~0.10 MB/s), yêu cầu cảnh báo trong vài giây.

## 3. Variety (Đa dạng)
- 4 nhóm dữ liệu: định danh, phân loại (Type: L/M/H), cảm biến liên tục, nhãn nhị phân đa nhãn.
- Thực tế bổ sung: sóng rung, ảnh nhiệt, log JSON, phiếu bảo trì dạng văn bản.

## 4. Veracity (Độ tin cậy)
- Giá trị khuyết: 0 | Dòng trùng lặp: 0
- Mất cân bằng nhãn: 339/10,000 ca hỏng (3.39%).
- Mâu thuẫn nhãn: 9 dòng hỏng không rõ nguyên nhân, 18 dòng có nguyên nhân nhưng không tính là hỏng.
- Nhiễu ngẫu nhiên RNF: 19 ca (0.19%) — giới hạn trần của mọi mô hình dự báo.

## 5. Value (Giá trị)
- Phát hiện sớm 80% số ca hỏng => tiết kiệm ước tính 25,764,000,000 VNĐ trên tệp mẫu.
- KPI đúng: Recall và F1-score (KHÔNG dùng Accuracy, vì mô hình ngây thơ đã đạt 96.61%).

## Kết luận
Xét riêng dung lượng, tệp mẫu chưa phải Big Data. Nhưng xét bản chất bài toán và hệ thống nguồn
sinh ra dữ liệu, đây là bài toán Big Data đầy đủ 5V — cần kiến trúc HDFS / Spark / Kafka khi triển khai thật.
