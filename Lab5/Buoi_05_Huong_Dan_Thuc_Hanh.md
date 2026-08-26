**BUỔI 05 — APACHE SPARK: XỬ LÝ TRONG BỘ NHỚ VÀ API PYSPARK**

**Khóa đào tạo:** Phân tích Dữ liệu Lớn (Big Data Analysis)

**Thời lượng:** 60 phút (Lý thuyết 25 phút + Thực hành 35 phút)

**Bộ dữ liệu:** `wordcount_corpus.txt` và `transactions.csv` — **dùng lại nguyên vẹn từ Buổi 04** để so sánh được thời gian chạy

**Môi trường:** PySpark ≥ 3.5 trên máy thật (chế độ `local[*]`) + Spark 3.5.1 trong cụm Docker để đối chứng công bằng với Hadoop

---

# 0. TỔNG QUAN BUỔI HỌC

## 0.1. Liên hệ với Buổi 04

Buổi 04 kết thúc bằng một con số khó chịu: **job MapReduce xử lý 7 KB và job xử lý 18,5 MB đều mất khoảng 30 giây.** Thời gian gần như **không phụ thuộc vào lượng dữ liệu** — nó bị chi phí cố định nuốt trọn.

Hôm nay ta chạy **đúng hai bài toán đó, trên đúng hai tệp đó**, bằng Apache Spark.

```text
BUỔI 04                                  BUỔI 05
Ghi trung gian xuống ĐĨA sau mỗi   ──►   Giữ trung gian trong BỘ NHỚ (RAM)
  giai đoạn Map/Reduce
Mỗi job = một chương trình riêng   ──►   Nhiều phép biến đổi trong một phiên
Viết mapper.py + reducer.py        ──►   .flatMap().map().reduceByKey()
Không có tối ưu tự động            ──►   Catalyst tối ưu lại kế hoạch giúp bạn
Hỏng thì chạy lại từ dữ liệu đĩa   ──►   Dựng lại từ LINEAGE, không cần ghi đĩa
~30 giây / job                     ──►   dưới 5 giây, cùng máy, cùng dữ liệu
```

> **Điểm sư phạm quan trọng nhất:** con số phải khớp **tuyệt đối** với Buổi 04 — **373** từ khác nhau, **1.144** tổng số từ, **206.744.802.000 VND** tổng doanh thu. Khi kết quả giống hệt mà thời gian giảm gần **mười lần**, học viên hiểu ngay: Spark **không tính khác**, Spark chỉ **tránh được việc ghi đĩa**.

## 0.2. Mục tiêu học tập

| **\#** | **Mục tiêu** | **Phần** |
|:---|:---|:---|
| 1 | Khởi tạo `SparkSession` đúng cách và đọc được Spark UI ở cổng 4040 | D1 |
| 2 | Viết WordCount bằng RDD API: `textFile` → `flatMap` → `map` → `reduceByKey` | D2 |
| 3 | Viết cùng phép tính bằng DataFrame API và giải thích vì sao nó nhanh hơn | D3 |
| 4 | Dùng Spark SQL trên dữ liệu phân tán với đúng cú pháp SQL của Buổi 03 | D4 |
| 5 | Phân biệt **transformation** với **action**, đọc được `explain()` và tìm ra `Exchange` | D5 |
| 6 | Dùng `cache()` / `persist()` đúng lúc và đo được lợi ích bằng số | D6 |
| 7 | So sánh hiệu năng Spark với MapReduce **một cách công bằng** | D7, D8 |
| 8 | Đọc được Spark UI: tab Jobs, Stages, SQL/DataFrame, Storage, Executors | 1.8, D2, D3, D6 |
| 9 | Giải thích được vì sao Spark chịu lỗi được mà **không cần** ghi đĩa như MapReduce | Lý thuyết |

## 0.3. Phân bổ thời gian

| **Khối** | **Thời lượng** | **Nội dung** |
|:---|:---|:---|
| **Lý thuyết** | 25 phút | Vì sao MapReduce chậm; kiến trúc Spark; RDD và lineage; lazy evaluation; Catalyst |
| **D1 – D2** | 8 phút | SparkSession, Spark UI, RDD WordCount đối chiếu Buổi 04 |
| **D3 – D4** | 10 phút | DataFrame API và Spark SQL |
| **D5 – D6** | 9 phút | Lazy evaluation, `explain()`, `cache()` |
| **D7 – D8** | 8 phút | So sánh hiệu năng, chốt bài |

> **Về mục 1.8 (bản đồ Spark UI):** đây là phần **tra cứu**, không nằm trong 60 phút trên lớp. Trên lớp chỉ cần mở sẵn `localhost:4040` và chỉ vào đúng vài chỗ khi chạy tới D2, D3, D6. Mọi ảnh trong tài liệu đều chụp từ một phiên Spark chạy đúng khối lượng công việc của buổi học, nên con số trên ảnh trùng khớp với con số học viên sẽ thấy.

## 0.4. Chuẩn bị trước giờ học

**Bước 1 — Bắt buộc: phải có `lab4/outputs/lab4_timings.json`.**

Buổi này **đọc trực tiếp** tệp đó để so sánh. Nếu chưa có, quay lại chạy Buổi 04 (chỉ cần đến D8).

**Bước 2 — Cài PySpark:**

```bash
pip install "pyspark>=3.5.1" findspark
```

**Bước 3 — Kiểm tra Java.** Spark chạy trên JVM:

```bash
java -version
```

| **Phiên bản PySpark** | **Java cần có** |
|:---|:---|
| 3.5.x | Java 8, 11 hoặc 17 |
| 4.x | **Java 17 trở lên** |

Không có Java thì `SparkSession.builder.getOrCreate()` sẽ báo `JAVA_HOME is not set` hoặc `Java gateway process exited`.

**Bước 4 — Khởi động cụm Docker** *(chỉ cần cho D8; D1–D7 chạy hoàn toàn trên máy thật)*:

```bash
docker compose up -d hadoop-namenode hadoop-datanode spark-master spark-worker
```

| **Dịch vụ** | **Địa chỉ** | **Vai trò** |
|:---|:---|:---|
| **Spark UI của phiên đang chạy** | **http://localhost:4040** | **Job, Stage, DAG, Storage — dùng suốt buổi** |
| Spark Master (cụm Docker) | http://localhost:8088 | Danh sách worker của cụm standalone |
| HDFS NameNode | http://localhost:9870 | Dữ liệu Buổi 04 vẫn nằm nguyên ở đây |

> **CẢNH BÁO QUAN TRỌNG NHẤT CỦA BUỔI HỌC — nói ngay từ phút đầu.**
>
> **Đừng nối PySpark trên máy thật vào cụm Spark trong Docker** bằng `.master("spark://localhost:7077")` trừ khi **phiên bản PySpark của bạn trùng khớp với phiên bản Spark của container** (`docker-compose.yml` của khóa học dùng **3.5.1**).
>
> Lệch phiên bản sẽ cho lỗi khó hiểu kiểu `java.io.InvalidClassException` hoặc treo vô hạn ở `Initial job has not accepted any resources`.
>
> Toàn bộ D1–D7 dùng `.master("local[*]")` — Spark chạy **ngay trong tiến trình Python của notebook**, mỗi lõi CPU là một "máy" ảo. Đây là chế độ đúng để học API, và cũng là chế độ mọi kỹ sư dữ liệu dùng khi phát triển. D8 sẽ chạy Spark **bên trong container** để so sánh công bằng với Hadoop.

---

# 1. PHẦN LÝ THUYẾT

## 1.1. Vì sao MapReduce chậm

Nhìn lại đúng những gì Buổi 04 đã làm, lần này chú ý vào các mũi tên chạm vào đĩa:

```text
MAPREDUCE — mỗi mũi tên "ĐĨA" là một lần ghi và đọc lại toàn bộ dữ liệu

HDFS ──đọc──► MAP ──ĐĨA──► SHUFFLE ──ĐĨA──► REDUCE ──ghi──► HDFS
                    ▲                  ▲
                    └── ghi ra đĩa cục bộ, rồi đọc lại qua mạng
```

Với **một** job thì chỉ tốn thêm hai lần chạm đĩa. Vấn đề xuất hiện khi bài toán cần **nhiều job nối tiếp** — mà hầu hết bài toán thật đều vậy:

```text
Bài toán: lọc → gom nhóm → nối bảng → xếp hạng   (4 job MapReduce)

MapReduce:  HDFS → job1 → HDFS → job2 → HDFS → job3 → HDFS → job4 → HDFS
                        ▲              ▲              ▲
              ba lần ghi + đọc lại toàn bộ dữ liệu trung gian, hoàn toàn thừa

Spark:      HDFS → biến đổi → biến đổi → biến đổi → biến đổi → HDFS
                            (tất cả trong RAM, không chạm đĩa)
```

Với thuật toán **lặp** — hồi quy logistic, PageRank, K-means — chênh lệch thành thảm họa: một thuật toán lặp 20 vòng phải ghi và đọc lại dữ liệu **20 lần**. Đây chính là bài toán khiến Spark ra đời tại AMPLab (Berkeley) năm 2009.

> **Nhưng đừng vội kết luận "ghi đĩa là ngu ngốc".** MapReduce ghi đĩa **có lý do**: khi một máy chết giữa chừng, dữ liệu trung gian **vẫn còn trên đĩa**, job chỉ cần chạy lại phần việc của máy đó. Đó là cách MapReduce chịu lỗi.
>
> Câu hỏi hay của buổi học: **nếu Spark không ghi đĩa thì nó chịu lỗi bằng cách nào?** Câu trả lời nằm ở mục 1.3 — và đó là ý tưởng đẹp nhất của Spark.

## 1.2. Kiến trúc Spark

```text
        ┌───────────────────────────────────────────────┐
        │  DRIVER  (tiến trình Python của bạn)          │
        │    SparkSession / SparkContext                │
        │    - giữ lineage của mọi RDD/DataFrame        │
        │    - chia job thành stage, stage thành task   │
        │    - KHÔNG giữ dữ liệu (trừ khi bạn collect)  │
        └───────────────────┬───────────────────────────┘
                            │  xin tài nguyên
                            ▼
        ┌───────────────────────────────────────────────┐
        │  CLUSTER MANAGER                              │
        │  local[*] · Standalone · YARN · Kubernetes    │
        └───────────────────┬───────────────────────────┘
                            │  cấp executor
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ EXECUTOR │  │ EXECUTOR │  │ EXECUTOR │
        │ task task│  │ task task│  │ task task│
        │ BỘ NHỚ   │  │ BỘ NHỚ   │  │ BỘ NHỚ   │  ◄── dữ liệu cache nằm đây
        └──────────┘  └──────────┘  └──────────┘
```

| **Thành phần** | **Vai trò** | **Tương ứng ở Buổi 04** |
|:---|:---|:---|
| **Driver** | Lập kế hoạch, điều phối, thu kết quả cuối | ApplicationMaster của YARN |
| **Cluster Manager** | Cấp phát tài nguyên | ResourceManager của YARN |
| **Executor** | Chạy task, giữ dữ liệu cache trong RAM | Container chạy Map/Reduce task |
| **Task** | Đơn vị công việc nhỏ nhất — **một task cho một phân vùng** | Một Map task cho một khối HDFS |

**Bốn tầng công việc — thuộc bốn từ này là đọc được Spark UI:**

```text
ỨNG DỤNG — một SparkSession là một ứng dụng, và là toàn bộ trang localhost:4040
 └─ JOB     — sinh ra bởi MỘT action (.collect(), .count(), .show(), .save())
     └─ STAGE  — một đoạn công việc KHÔNG có shuffle bên trong.
        │        Ranh giới giữa hai stage LUÔN LUÔN là một lần shuffle.
        └─ TASK   — một stage chạy trên một phân vùng. 8 phân vùng = 8 task.
```

> **Chế độ `local[*]` nghĩa là gì?** Driver và Executor nằm **chung một tiến trình JVM**, và `*` = dùng hết số lõi CPU. Trên máy 12 lõi, `defaultParallelism = 12` — Spark coi mỗi lõi như một "máy". Mọi khái niệm phân vùng, stage, shuffle đều **hoạt động thật**, chỉ là dữ liệu không đi qua mạng. Đây là lý do `local[*]` đủ để học trọn API.

---

### Phân vùng — đơn vị song song thật sự của Spark

Bảng thành phần ở trên có một dòng rất dễ đọc lướt qua: **một task cho một phân vùng**. Đó là toàn bộ cơ chế song song của Spark. Số phân vùng quyết định số task, và số task quyết định bạn dùng được bao nhiêu phần trăm số lõi đang có.

| **Số phân vùng** | **Chuyện gì xảy ra trên máy 12 lõi** | **Hậu quả** |
|:---|:---|:---|
| Quá ít — ví dụ 2 | Spark chỉ sinh 2 task, 10 lõi ngồi không | Chậm hơn nhiều lần so với khả năng thật của máy |
| Vừa đủ — 24 đến 48 | Mỗi lõi nhận 2–4 task nối tiếp nhau | Lõi nào xong sớm nhận việc tiếp, tải cân bằng |
| Quá nhiều — 200 cho 18,5 MB | Mỗi task chỉ xử lý chưa tới 100 KB | Chi phí lập lịch và khởi động task lấn át công việc thật |

**Quy tắc thực dụng: 2–4 phân vùng cho mỗi lõi.** Lý do phải nhiều hơn một phân vùng mỗi lõi: dữ liệu không bao giờ chia đều tuyệt đối, nên cần dư task để lõi rảnh còn có việc nhận thêm.

Xem số phân vùng hiện tại:

```python
dong.getNumPartitions()      # với RDD
df.rdd.getNumPartitions()    # với DataFrame
```

Đổi số phân vùng có hai lệnh, và chọn nhầm là trả giá bằng nguyên một lần shuffle:

| **Lệnh** | **Tăng được?** | **Giảm được?** | **Có shuffle?** | **Dùng khi** |
|:---|:---|:---|:---|:---|
| `repartition(n)` | Có | Có | **Có — đắt** | Cần chia lại cho đều, hoặc cần tăng độ song song |
| `coalesce(n)` | Không | Có | Không | Gộp bớt phân vùng nhỏ trước khi ghi ra tệp |

> **Điểm dạy dễ nhầm nhất, phải nói rõ ngay từ đầu:** số phân vùng của Spark **không liên quan** tới số khối HDFS của Buổi 04. `transactions.csv` nằm trên HDFS thành **3 khối** (8.388.608 + 8.388.608 + 2.574.823 byte), nhưng khi Spark đọc đúng tệp đó ở chế độ `local[*]` nó cho **5 phân vùng**.
>
> Khối là đơn vị **lưu trữ**, do HDFS quyết định lúc ghi. Phân vùng là đơn vị **tính toán**, do Spark quyết định lúc đọc, dựa trên kích thước tệp và số lõi. Hai con số này có thể trùng nhau trong vài trường hợp riêng, nhưng đó là trùng hợp chứ không phải quy luật.


## 1.3. RDD và lineage — cách Spark chịu lỗi mà không ghi đĩa

**RDD** (*Resilient Distributed Dataset*) có ba tính chất, và mỗi tính chất giải một vấn đề:

| **Tính chất** | **Nghĩa là** | **Giải quyết** |
|:---|:---|:---|
| **Phân tán** (Distributed) | Chia thành nhiều **phân vùng** nằm trên nhiều executor | Sức chứa và tốc độ |
| **Bất biến** (Immutable) | Tạo ra rồi thì **không sửa được**; mọi phép biến đổi sinh RDD **mới** | Chạy lại luôn cho cùng kết quả |
| **Có khả năng hồi phục** (Resilient) | Nhớ **cách mình được tạo ra**, nên dựng lại được | Chịu lỗi **không cần ghi đĩa** |

**Lineage** (phả hệ) là chuỗi biến đổi tạo ra một RDD:

```text
textFile("corpus.txt")
    │  flatMap(tách từ)
    ▼
  RDD_1
    │  map(w -> (w,1))
    ▼
  RDD_2
    │  reduceByKey(cộng)
    ▼
  RDD_3   ◄── phân vùng số 2 của RDD_3 nằm trên executor vừa chết
```

Executor chết, mất phân vùng số 2. Spark **không** chạy lại cả job. Nó đọc lineage, thấy phân vùng đó được tạo từ đâu, và **chỉ tính lại đúng phân vùng đó** trên một executor khác.

> **Đây là ý tưởng cốt lõi của Spark, và đáng dành 3 phút để cả lớp thấm:**
>
> MapReduce chịu lỗi bằng cách **ghi lại dữ liệu** (tốn đĩa, tốn thời gian, làm mọi job chậm đi kể cả khi không có máy nào chết).
>
> Spark chịu lỗi bằng cách **ghi lại công thức** (lineage chỉ là vài dòng siêu dữ liệu trong RAM của Driver — gần như miễn phí).
>
> Ghi công thức rẻ hơn ghi dữ liệu hàng triệu lần. **Toàn bộ lợi thế tốc độ của Spark bắt nguồn từ nhận xét này.**

## 1.4. Lazy evaluation — biến đổi và hành động

Spark chia mọi phép toán làm **hai loại**, và phân biệt được hai loại này là điều kiện cần để hiểu Spark:

| | **TRANSFORMATION (biến đổi)** | **ACTION (hành động)** |
|:---|:---|:---|
| Trả về | Một RDD/DataFrame **mới** | Một giá trị Python, hoặc ghi ra tệp |
| Khi gọi | **KHÔNG tính gì cả** — chỉ ghi vào lineage | **Kích hoạt** toàn bộ chuỗi đã ghi |
| Ví dụ | `map`, `filter`, `flatMap`, `groupBy`, `join`, `select`, `withColumn` | `collect`, `count`, `show`, `take`, `first`, `save`, `write` |

```python
df2 = df.filter(...)          # 0 giây — chưa tính gì
df3 = df2.withColumn(...)     # 0 giây — chưa tính gì
df4 = df3.groupBy(...).sum()  # 0 giây — chưa tính gì
df4.show()                    # ◄── TẤT CẢ chạy ở đây
```

> **Lười để làm gì?** Vì khi `show()` được gọi, Spark **nhìn thấy trọn kế hoạch** chứ không phải từng bước rời rạc. Nhìn thấy trọn kế hoạch thì tối ưu được:
>
> - **Đẩy bộ lọc xuống sát nguồn** (*predicate pushdown*): nếu cuối cùng chỉ cần `quantity > 3`, hãy lọc **ngay lúc đọc tệp** thay vì đọc hết rồi mới lọc.
> - **Cắt bớt cột** (*column pruning*): chỉ dùng 3 trên 10 cột thì chỉ đọc 3 cột.
> - **Gộp nhiều phép vào một lần duyệt** (*pipelining*): `filter` rồi `map` rồi `filter` chạy trong **một** vòng lặp, không phải ba.
>
> Đây chính xác là điều Buổi 03 gọi là **bộ tối ưu truy vấn** của PostgreSQL, nay áp dụng cho dữ liệu phân tán.

**Narrow và Wide — hai loại biến đổi, khác nhau ở chỗ có shuffle hay không:**

```text
NARROW (hẹp) — mỗi phân vùng vào sinh ra đúng một phân vùng ra.
               KHÔNG có dữ liệu đi qua mạng. RẺ.
   P1 ──► P1'
   P2 ──► P2'          map, filter, flatMap, select, withColumn, union
   P3 ──► P3'

WIDE (rộng) — một phân vùng ra cần dữ liệu từ NHIỀU phân vùng vào.
              PHẢI shuffle qua mạng. ĐẮT. Là ranh giới giữa hai STAGE.
   P1 ──┐
   P2 ──┼──► P1'       groupByKey, reduceByKey, join, distinct, orderBy, repartition
   P3 ──┘
```

> **Kỹ năng cần rèn:** nhìn một đoạn code là biết nó shuffle mấy lần. Mỗi lần shuffle là một stage mới, và stage là đơn vị mà Spark UI hiển thị. Trong `explain()`, shuffle hiện ra dưới cái tên **`Exchange`** — hãy đếm số dòng `Exchange`.
>
> Và nhớ lại Buổi 04: **Shuffle & Sort là giai đoạn tốn kém nhất của MapReduce.** Nó cũng là giai đoạn tốn kém nhất của Spark. Spark **không xóa được** shuffle — nó chỉ xóa được **những lần ghi đĩa giữa các job**.

---

### AQE — Spark 3 tự sửa kế hoạch trong lúc chạy

Lazy evaluation cho Catalyst nhìn thấy trọn kế hoạch **trước khi** chạy. Nhưng trước khi chạy thì Catalyst mới chỉ **ước lượng** được kích thước dữ liệu. Từ Spark 3, **Adaptive Query Execution (AQE)** bổ sung thêm một tầng nữa: sau mỗi lần shuffle, Spark **đo kích thước thật** rồi viết lại phần kế hoạch còn lại. AQE **bật mặc định** — tham số `spark.sql.adaptive.enabled` để `true`, và lời khuyên là giữ nguyên.

Ba việc AQE làm:

| **Việc** | **Cơ chế** | **Dấu vết nhìn thấy được** |
|:---|:---|:---|
| Gộp phân vùng nhỏ sau shuffle | Các phân vùng gần như rỗng sau shuffle được gộp lại thành vài phân vùng có kích thước hợp lý | Dòng `AQEShuffleRead coalesced` trên đồ thị kế hoạch |
| Đổi kiểu join | Phát hiện một vế nhỏ hơn ngưỡng, đổi **sort-merge join** thành **broadcast join**, bỏ hẳn việc shuffle bảng lớn | `BroadcastHashJoin` thay cho `SortMergeJoin` |
Thay hàng ở dòng 309 bằng:

| Tách phân vùng lệch trong JOIN (skew join) | **Chỉ áp dụng cho join có shuffle** (sort-merge join / shuffled hash join): phân vùng bị lệch được chẻ thành nhiều mảnh theo dải map-output, và phân vùng tương ứng ở vế bên kia được **nhân bản** cho từng mảnh nên kết quả vẫn đúng. Do một tham số **riêng** điều khiển: `spark.sql.adaptive.skewJoin.enabled` (mặc định `true`). **Không** chữa được skew của `groupBy`/aggregate — chỗ đó vẫn phải tách khóa nóng hoặc salting | Nút join trong kế hoạch mang nhãn `SortMergeJoin(skew=true)`; số task nhiều hơn số phân vùng đã đặt |

Và sửa luôn dòng 381 cho khớp (nếu không thì mâu thuẫn nội bộ vẫn còn):

1. **Bật AQE skew join — nhưng chỉ cứu được join.** Với `spark.sql.adaptive.enabled` và `spark.sql.adaptive.skewJoin.enabled` cùng để `true` (cả hai đã mặc định bật), Spark tự chẻ phân vùng lệch của một phép **join có shuffle**. Rẻ nhất, nên thử trước tiên — nhưng nếu chỗ lệch nằm ở `groupBy`/`reduceByKey` như hai bài của buổi này thì AQE **không đỡ được**, phải đi tiếp xuống cách 2 hoặc 3.

Điều này dạy học viên một chuyện quan trọng: con số `spark.sql.shuffle.partitions` mà bạn đặt là một **gợi ý ban đầu**, không phải mệnh lệnh cuối cùng. AQE sẽ sửa lại theo dữ liệu thật đo được lúc chạy.

> **Hệ quả thực tế mà lớp sẽ đụng ngay ở D5 — hãy nói trước để không ai hoang mang:** nếu một DataFrame **đã từng chạy qua một action**, `explain()` in ra **hai** kế hoạch chứ không phải một:
>
> - `== Final Plan ==` — kế hoạch **sau khi** AQE chỉnh lại theo kích thước dữ liệu thật.
> - `== Initial Plan ==` — kế hoạch Catalyst dựng **trước khi** chạy.
>
> Vì vậy, đếm chuỗi `"Exchange"` trên **toàn bộ** đầu ra sẽ ra **4** thay vì **2**: vẫn đúng hai lần shuffle đó, nhưng bị đếm hai lượt ở hai kế hoạch. Học viên nào ra 4 thì **không sai phép đếm** — chỉ đếm nhầm phạm vi. Cách xử lý: cắt lấy phần sau `== Initial Plan ==` rồi mới đếm.


## 1.5. DataFrame, Catalyst và vì sao nên bắt đầu từ DataFrame

**RDD** là tập hợp các đối tượng Python — Spark **không biết** bên trong có gì.
**DataFrame** là bảng có **lược đồ**: tên cột, kiểu dữ liệu. Spark biết rõ nó chứa gì.

Biết lược đồ mở ra ba lợi thế:

| **Lợi thế** | **Cơ chế** |
|:---|:---|
| **Tối ưu tự động** | **Catalyst** viết lại kế hoạch của bạn cho nhanh hơn — pushdown, pruning, đổi thứ tự join |
| **Lưu trữ hiệu quả** | **Tungsten** giữ dữ liệu ở dạng nhị phân ngoài heap JVM, không phải đối tượng Java |
| **Không trả giá cho Python** | Phép toán DataFrame chạy **trong JVM**. RDD với `lambda` Python phải **serialize từng dòng** qua lại giữa JVM và Python — cực đắt |

> **Lời khuyên nghề nghiệp, nói thẳng cho học viên:**
>
> **Mặc định dùng DataFrame / Spark SQL.** Chỉ dùng RDD khi thật sự cần điều DataFrame không làm được — dữ liệu phi cấu trúc, thuật toán tùy biến trên từng phân vùng.
>
> Buổi này vẫn dạy RDD (D2) vì **RDD là chỗ duy nhất nhìn thấy Map và Reduce của Buổi 04 xuất hiện nguyên hình**: `flatMap` chính là mapper WordCount, `reduceByKey` chính là reducer. Hiểu RDD thì hiểu DataFrame đang che giấu điều gì.

---

### Broadcast join — mẹo tối ưu dùng nhiều nhất trong thực tế

Bài toán quen thuộc đến mức gần như ngày nào cũng gặp: nối bảng giao dịch **200.000 dòng** với một bảng danh mục cửa hàng chỉ **10 dòng**. Mặc định, `join` là phép **wide**: Spark băm khóa nối rồi shuffle **cả hai** bảng để các dòng cùng khóa gặp nhau trên cùng một executor. Nghĩa là toàn bộ 200.000 dòng phải đi qua mạng chỉ để gặp 10 dòng.

Broadcast join đảo ngược cách làm: gửi **nguyên bảng nhỏ** tới **mọi** executor, rồi mỗi executor nối cục bộ với phần dữ liệu lớn nó đang giữ sẵn. **Bảng lớn không nhúc nhích một dòng nào.**

```python
from pyspark.sql import functions as F
ket_qua = df_lon.join(F.broadcast(df_nho), on="store_id", how="left")
```

| | **Join mặc định (sort-merge)** | **Broadcast join** |
|:---|:---|:---|
| Bảng lớn | Bị shuffle toàn bộ theo khóa | **Không di chuyển** |
| Bảng nhỏ | Bị shuffle theo khóa | Được gửi nguyên bản tới mọi executor |
| Dấu vết trong `explain()` | **Hai** dòng `Exchange hashpartitioning` | Một dòng `BroadcastExchange` |
| Ràng buộc | Không có | Bảng nhỏ phải vừa RAM của **mỗi** executor |

Spark tự chọn broadcast khi ước lượng bảng nhỏ nằm dưới ngưỡng `spark.sql.autoBroadcastJoinThreshold` — **mặc định 10 MB**. Nhưng chữ "ước lượng" chính là chỗ hỏng: sau một chuỗi `filter` và `join`, Spark thường **đánh giá quá cao** kích thước thật và bỏ lỡ cơ hội; với bảng đọc thẳng từ CSV thì lại càng thiếu thống kê để ước lượng cho đúng. Khi đó phải **ép thủ công** bằng `F.broadcast()`.

> **Điểm dạy:** đây là ví dụ rõ nhất cho chuyện **đọc `explain()` sinh ra tiền**. Cùng một dòng `join`, chỉ khác một lời gợi ý cho bộ tối ưu, mà một bên đẩy toàn bộ bảng lớn qua mạng còn một bên gần như không đẩy gì.
>
> Hãy rèn thói quen: mỗi khi viết `join` với một bảng danh mục, tự hỏi **"bảng nhỏ này có broadcast được không"** trước khi bấm chạy, rồi kiểm chứng bằng `explain()` — tìm `BroadcastExchange` thay vì hai `Exchange`.

---

### Data skew — một khóa làm chậm cả job

Shuffle chia dữ liệu theo khóa. Cách chia đó chỉ hiệu quả khi các khóa mang **lượng dữ liệu tương đương nhau**. Nếu một khóa chiếm phần lớn dữ liệu, task xử lý khóa đó chạy lâu gấp nhiều lần các task khác — và **job chỉ xong khi task chậm nhất xong**. Mười một task xong trong 2 giây, task thứ mười hai chạy 3 phút, thì job mất 3 phút. Thêm máy vào cụm cũng không cứu được, vì một khóa thì không tự chia nhỏ ra được.

**Cách phát hiện — đừng đoán, hãy đọc số:** mở `localhost:4040`, vào tab **Stages**, chọn stage đang nghi ngờ, rồi xem bảng **Summary Metrics for Completed Tasks**.

| **Chỉ số cần so** | **Bình thường** | **Có skew** |
|:---|:---|:---|
| `Duration` — cột Median so với cột Max | Max lớn hơn Median vài chục phần trăm | Max gấp hàng chục lần Median |
| `Shuffle Read Size` — Median so với Max | Hai cột xấp xỉ nhau | Một task đọc gấp nhiều lần phần còn lại |

Ba hướng xử lý, xếp theo công sức phải bỏ ra:

1. **Bật AQE skew join** — với `spark.sql.adaptive.enabled` để `true`, Spark tự chẻ phân vùng lệch thành nhiều phần. Rẻ nhất, nên thử trước tiên.
2. **Tách riêng khóa nóng** — xử lý vài khóa lớn bằng một nhánh riêng, phần còn lại bằng nhánh thường, rồi `union` hai kết quả lại.
3. **Salting** — thêm hậu tố ngẫu nhiên vào khóa (`"HCM"` thành `"HCM_0"` cho tới `"HCM_9"`) để một khóa nóng trải ra nhiều phân vùng, gom nhóm hai lần rồi bỏ hậu tố. Mạnh nhất nhưng phải sửa code.

Thay toàn bộ dòng 385 bằng (giữ nguyên vế cuối, dùng đúng cú pháp tham chiếu như dòng 921):

> **Nối với Buổi 04:** đây chính là hiện tượng đã gặp ở bài tập **"Nhiều Reducer"** (Buổi 04, Phụ lục A bài 4) — chia việc cho ba reducer nhưng ba tệp `part-` sinh ra không hề đều nhau, vì các khóa không mang lượng dữ liệu như nhau., vì các khóa không mang lượng dữ liệu như nhau.
>
> Spark đổi tên gọi từ *reducer* thành *task*, nhưng bản chất bài toán **không đổi một chút nào**: xử lý phân tán chỉ nhanh khi việc được chia đều. Đây là điều học viên cần mang theo sang mọi hệ phân tán khác, không riêng gì Spark.


## 1.6. `cache()` và `persist()`

Hệ quả trực tiếp của lazy evaluation, và cũng là cái bẫy số một của người mới:

```python
df2 = df.filter(...).withColumn(...)
df2.count()   # đọc lại tệp gốc, tính lại từ đầu
df2.count()   # ĐỌC LẠI TỆP GỐC, TÍNH LẠI TỪ ĐẦU MỘT LẦN NỮA
```

RDD/DataFrame **không tự nhớ kết quả**. Mỗi action chạy lại trọn lineage. Muốn nhớ, phải nói rõ:

```python
df2.cache()      # giữ kết quả lại — xem bảng StorageLevel bên dưới
df2.count()      # tính thật, và giữ kết quả lại trong RAM
df2.count()      # lấy thẳng từ RAM — nhanh hơn nhiều
df2.unpersist()  # trả lại bộ nhớ khi không cần nữa
```

| **StorageLevel** | **Nghĩa** | **Dùng khi** |
|:---|:---|:---|
| `MEMORY_ONLY` | Chỉ RAM; không đủ thì phân vùng thừa bị **bỏ và tính lại** | RDD nhỏ, tính lại rẻ |
| `MEMORY_AND_DISK_DESER` | Ưu tiên RAM, thiếu chỗ mới tràn xuống đĩa; giữ nguyên dạng đối tượng nên đọc nhanh, tốn RAM hơn | **Đây mới là mức mà `DataFrame.cache()` thật sự dùng** |
| `MEMORY_AND_DISK` | Như trên nhưng giữ ở dạng **đã tuần tự hóa**: tốn ít RAM hơn, tốn CPU hơn | RAM chật |
| `DISK_ONLY` | Chỉ đĩa | Tính lại rất đắt mà RAM không còn |

> **Một cái bẫy có thật của PySpark — học viên sẽ đụng ngay ở D6.** Trong PySpark, hằng số `StorageLevel.MEMORY_AND_DISK` là bản **đã tuần tự hóa**, còn `DataFrame.cache()` lại gọi xuống JVM và dùng bản **giữ nguyên đối tượng**. Hai câu lệnh dưới đây cho ra hai mức khác nhau, đã đo trên chính cụm của khóa học:
>
> ```python
> co_so.cache()                                # -> Disk Memory Deserialized 1x Replicated
> co_so.persist(StorageLevel.MEMORY_AND_DISK)  # -> Disk Memory Serialized   1x Replicated
> ```
>
> Vì thế tab **Storage** ở D6 hiện chữ `Deserialized`. Đổi `cache()` thành `persist(StorageLevel.MEMORY_AND_DISK)` rồi xem lại tab đó là cách kiểm chứng nhanh nhất.

> **Ba quy tắc dùng `cache()`:**
>
> 1. **Chỉ cache thứ được dùng LẠI ít nhất hai lần.** Cache thứ dùng một lần là **lãng phí thuần túy** — tốn RAM, không được gì.
> 2. **Cache SAU khi đã lọc và chọn cột**, đừng cache bảng thô. Cache 10 cột trong khi chỉ dùng 3 là ném đi 70% bộ nhớ.
> 3. **`unpersist()` khi xong.** RAM của executor là tài nguyên chung; dữ liệu cache chiếm chỗ của dữ liệu đang xử lý và có thể làm job khác chậm đi.

---

### Parquet — định dạng nên dùng cho mọi dữ liệu trung gian

`cache()` giữ dữ liệu trong RAM của **phiên hiện tại** — đóng phiên là mất sạch. Khi cần giữ kết quả qua nhiều phiên, hoặc chuyển kết quả từ bước này sang bước khác của đường ống dữ liệu, hãy ghi ra tệp. Và định dạng nên chọn là **Parquet**, không phải CSV.

CSV lưu **theo dòng**, mọi thứ đều là chuỗi ký tự. Parquet lưu **theo cột**, có nén, và mang **lược đồ nhúng bên trong** tệp. Ba hệ quả:

1. **Chỉ đọc cột cần dùng.** Truy vấn dùng 3 trên 10 cột thì Spark chỉ chạm vào 3 khối cột — chính là `column pruning` mà `explain()` ở D5 chỉ ra. Với CSV, muốn lấy một cột vẫn phải đọc và phân tách cả dòng.
2. **Nén tốt hơn nhiều.** Dữ liệu trong cùng một cột thì cùng kiểu và thường lặp lại — 6 giá trị `category` cho 200.000 dòng — nên nén rất hiệu quả. Xếp theo dòng thì mỗi dòng là một hỗn hợp kiểu, nén kém hẳn.
3. **Đọc lại không cần `inferSchema`.** Kiểu dữ liệu nằm sẵn trong tệp, nên Spark **không phải quét tệp hai lần** như cái bẫy đã nêu ở D1 — và cột ngày tháng đọc lên vẫn là ngày tháng, không âm thầm biến thành chuỗi.

```python
doanh_thu.write.mode("overwrite").parquet("lab5/outputs/doanh_thu_parquet")

dt = spark.read.parquet("lab5/outputs/doanh_thu_parquet")   # không cần inferSchema
dt.printSchema()
```

> **Quy tắc nghề nghiệp, đáng chép vào sổ:** **CSV để trao đổi** với người và với hệ thống ngoài — ai cũng mở được. **Parquet để lưu trữ và xử lý** — dùng cho mọi dữ liệu trung gian giữa các bước.
>
> Điều này dạy học viên rằng chọn định dạng lưu trữ là một **quyết định hiệu năng**, ngang hàng với việc quyết định `cache()` hay đặt số phân vùng, chứ không phải chuyện quy ước cho gọn thư mục. Buổi 06 và Buổi 13 sẽ dùng Parquet cho toàn bộ dữ liệu trung gian.


## 1.7. Bảng đối chiếu MapReduce ↔ Spark

| **Buổi 04 — MapReduce** | **Buổi 05 — Spark RDD** | **Buổi 05 — DataFrame / SQL** |
|:---|:---|:---|
| `mapper.py` tách dòng thành từ | `.flatMap(lambda d: d.split())` | `explode(split(col, " "))` |
| `mapper.py` phát `<khóa, giá trị>` | `.map(lambda w: (w, 1))` | `select(...)` |
| Shuffle & Sort (Hadoop tự làm) | Ngầm trong `reduceByKey` | Ngầm trong `groupBy` |
| `reducer.py` cộng dồn theo khóa | `.reduceByKey(lambda a, b: a + b)` | `.groupBy("k").agg(sum(...))` |
| `-D mapreduce.job.reduces=3` | `.reduceByKey(f, numPartitions=3)` | `spark.sql.shuffle.partitions = 3` |
| Ghi trung gian xuống đĩa | Giữ trong RAM | Giữ trong RAM |
| Chịu lỗi bằng dữ liệu trên đĩa | Chịu lỗi bằng **lineage** | Chịu lỗi bằng **lineage** |
| Xem job ở `localhost:8089` | Xem job ở **`localhost:4040`** | Xem job ở **`localhost:4040`** |
| `part-00000` trên HDFS | `.saveAsTextFile()` | `.write.parquet()` |

---

## 1.8. Bản đồ Spark UI — bảy tab và bốn tầng công việc

Spark UI là **công cụ chẩn đoán quan trọng nhất của nghề này**. Kỹ sư dữ liệu có kinh nghiệm không ngồi đoán vì sao job chạy lâu — người đó mở trình duyệt, đọc số, rồi mới sửa code. Tài liệu này dùng Spark UI ở **gần như mọi bước D**: D1 mở trang lần đầu khi chưa có job nào, D2 nhìn job đầu tiên xuất hiện, D3 và D4 xem kế hoạch truy vấn, D5 đối chiếu số dòng `Exchange` với ranh giới stage, D6 kiểm chứng `cache()` bằng tab Storage. Học viên nào chỉ đọc kết quả in ra màn hình mà không mở 4040 thì mới học được **một nửa** buổi này.

> **Nhắc lại điều dễ quên nhất:** cổng 4040 **chỉ sống khi `SparkSession` còn sống**. Gọi `spark.stop()`, hoặc tắt kernel của notebook, là trang đó chết ngay cùng toàn bộ lịch sử job. Vì vậy hãy **chụp màn hình trước khi đóng phiên** — sản phẩm phải nộp của buổi học yêu cầu đúng những ảnh đó. Nếu 4040 đang bị một phiên khác chiếm, Spark tự nhảy sang 4041, 4042; đó **không phải lỗi**.

Bảy tab của Spark UI, mỗi tab trả lời đúng một câu hỏi:

| **Tab** | **Câu hỏi nó trả lời** | **Khái niệm lý thuyết tương ứng** |
|:---|:---|:---|
| **Jobs** | Chương trình đã kích hoạt bao nhiêu action, mỗi action mất bao lâu? | Action và lazy evaluation — mục 1.4 |
| **Stages** | Mỗi job bị cắt thành mấy đoạn, mỗi đoạn đẩy bao nhiêu byte qua shuffle? | Narrow và wide, ranh giới stage — mục 1.4 |
| **Storage** | Cái gì đang thực sự nằm trong RAM, chiếm bao nhiêu, đã nạp đủ phân vùng chưa? | `cache()` và `persist()` — mục 1.6 |
| **Environment** | Cấu hình nào **đang thực sự có hiệu lực**, phiên bản Spark là bao nhiêu? | Tham số `spark.sql.shuffle.partitions` đặt ở D1 |
| **Executors** | Có mấy tiến trình đang chạy task, mỗi tiến trình bao nhiêu lõi và bao nhiêu RAM? | Driver, Cluster Manager, Executor — mục 1.2 |
| **SQL / DataFrame** | Mỗi truy vấn sinh ra kế hoạch nào, mỗi nút trong kế hoạch xử lý bao nhiêu dòng? | Catalyst — mục 1.5; `explain()` — D5 |
| **Structured Streaming** | Mỗi lô nhỏ nạp vào bao nhiêu bản ghi và xử lý mất bao lâu? *(chỉ hiện khi chương trình có truy vấn luồng)* | Buổi 08 trở đi |

> **Bốn tầng công việc của mục 1.2 nằm gọn trong ba tab đầu tiên.** **Ứng dụng** là toàn bộ trang 4040 — một `SparkSession` là một ứng dụng. **Job** là một dòng ở tab Jobs. **Stage** là một dòng ở tab Stages. **Task** là con số trong cột `Tasks: Succeeded/Total` của cả hai bảng.
>
> Đi từ trái sang phải trên thanh tab cũng chính là đi từ tầng thô xuống tầng chi tiết, và đó là **đúng thứ tự nên dùng khi gỡ lỗi**: xem job nào lâu, rồi mới xem stage nào bên trong job đó lâu, rồi mới xem task nào lệch so với các task còn lại.

### Tab Jobs — mỗi dòng là một action

![Tab Jobs của Spark UI ở cổng 4040: hãy đếm số dòng trong bảng Completed Jobs rồi đối chiếu với số lần bạn đã gọi action, và đọc cột Stages Succeeded/Total để biết mỗi job bị cắt thành mấy stage](images/14_spark_jobs.png)

Bảng `Completed Jobs` có sáu cột: `Job Id`, `Description`, `Submitted`, `Duration`, `Stages: Succeeded/Total` và `Tasks (for all stages): Succeeded/Total`. Bảng `Completed Jobs` có sáu cột: `Job Id`, `Description`, `Submitted`, `Duration`, `Stages: Succeeded/Total` và `Tasks (for all stages): Succeeded/Total`. Cột `Description` chính là chỗ đáng nhìn nhất — nhưng nó **không ghi trơ tên action**, mà ghi **nơi action được gọi**, theo dạng `<tên action> at <tệp>:<dòng>`. Trong ảnh trên, gần như mọi dòng đều là `collect at .../phien_spark.py:57`: đó là `.collect()` trong vòng lặp đo cache của D6, gọi đi gọi lại 8 vòng nên sinh ra một loạt job giống hệt nhau. Cuộn tiếp xuống những `Job Id` nhỏ hơn, bạn sẽ gặp `count at ...`, `takeOrdered at ...`, `show at ...` của D2 và D3, và `save at ...` của bước ghi Parquet.

> **Một dòng lạ cần giải thích ngay:** `$anonfun$withThreadLocalCaptured$1 at CompletableFuture.java:1768`. Đây vẫn là một job thật, nhưng **không phải action nào bạn gọi**. Spark tự phát nó trên một luồng nền trong lúc thực thi truy vấn (broadcast, hoặc truy vấn con của AQE). Vì điểm gọi rơi vào mã nội bộ của Spark chứ không vào tệp Python của bạn, `Description` đành ghi tên hàm nội bộ của JVM. Gặp dạng `$anonfun$...` thì **đừng đi tìm action tương ứng trong code của mình** — và nhớ trừ những dòng này ra khi đối chiếu số job với số action đã gọi.

Điểm dạy quan trọng nhất của tab này nằm ở **những dòng không có mặt**: không một dòng nào sinh ra bởi `map`, `filter`, `withColumn` hay `groupBy`. Học viên đã viết hàng chục phép biến đổi như vậy ở D2 và D3, nhưng Spark UI **không ghi nhận một dòng nào** cho chúng.

> **Điều này dạy học viên điều gì?** Đây là **bằng chứng nhìn thấy được** cho lazy evaluation ở mục 1.4. Ở D5 học viên đo được rằng ba phép biến đổi mất khoảng 3 mili giây còn một `collect()` mất khoảng 480 mili giây — đó là bằng chứng bằng **đồng hồ**. Tab Jobs là bằng chứng bằng **mắt**: transformation không tồn tại đối với bộ lập lịch của Spark, chúng chỉ là các dòng ghi trong lineage nằm ở Driver. Chỉ khi một action được gọi thì Spark mới dựng kế hoạch, chia stage, phát task — và mới đẻ ra một dòng ở đây.
>
> Thay toàn bộ dòng 494 bằng:

> **Cách kiểm chứng ngay tại lớp:** đếm số action đã gọi từ đầu phiên, rồi so với số dòng trong bảng. Quy tắc đúng cần nhớ là **số job luôn lớn hơn hoặc bằng số action**, không bao giờ nhỏ hơn — mỗi action sinh ra *ít nhất* một job. Phần dôi ra có hai nguồn quen thuộc, cả hai đều có mặt ngay trong bài này: `inferSchema=True` đọc tệp thêm một lượt để đoán kiểu nên sinh job riêng; và một truy vấn có shuffle chạy dưới **AQE** bị cắt thành nhiều job — chính vì thế trang chi tiết Query 2 ở D3 ghi `Succeeded Jobs 3, 4`, tức một lệnh `show()` sinh ra hai job.

### Tab Stages — ranh giới stage chính là shuffle

![Bảng Completed Stages: hãy đọc hai cột Shuffle Write và Shuffle Read cạnh nhau, con số Shuffle Write của một stage xuất hiện lại nguyên vẹn ở cột Shuffle Read của stage kế tiếp](images/15_spark_stages.png)

Bảng `Completed Stages` có các cột `Stage Id`, `Description`, `Tasks: Succeeded/Total`, `Input`, `Output`, `Shuffle Read` và `Shuffle Write`. Hai cột cuối là toàn bộ giá trị của tab này, và cách đọc đúng là **đọc chúng theo cặp, giữa hai stage liền nhau**:

1. **`Shuffle Write` của stage N** là lượng dữ liệu stage đó ghi ra để giao cho stage sau.
2. **`Shuffle Read` của stage N+1** là lượng dữ liệu stage sau nhận vào.
3. Hai con số này **bằng đúng nhau**. Ví dụ ở WordCount của D2: stage `reduceByKey` ghi ra 4.545 byte, và stage kế tiếp làm nhiệm vụ `count` đọc vào đúng 4.545 byte.

> **Điều này dạy học viên điều gì?** Mỗi cặp `Shuffle Write` — `Shuffle Read` như vậy **chính là một dòng `Exchange`** trong `explain()` ở D5. Ba cách nhìn khác nhau vào cùng một sự việc:
>
> - Trong **lý thuyết** ở mục 1.4, đó là một phép biến đổi **wide**.
> - Trong **`explain()`**, đó là một dòng `Exchange hashpartitioning(...)`.
> - Trong **tab Stages**, đó là một đường kẻ giữa hai dòng, kèm số byte thật đã đi qua.
>
> Kế hoạch WIDE của D5 có đúng **2** dòng `Exchange`, nên nó phải cho ra **3** stage và **2** cặp shuffle. Bắt học viên đối chiếu ba chỗ này với nhau là cách chắc chắn nhất để khái niệm shuffle thôi trừu tượng.
>
> **Và đây là con số nên tin khi đo hiệu năng.** Phụ lục A bài 3 đã chỉ ra: trên 7 KB dữ liệu, `reduceByKey` và `groupByKey` chạy nhanh gần như nhau vì nhiễu đo che hết. Nhưng cột `Shuffle Write` thì không nói dối — một bên đẩy 373 cặp, một bên đẩy 1.144 cặp. Đồng hồ bị nhiễu đo che lấp; khối lượng dữ liệu qua shuffle thì không.

### Tab Executors — vì sao chỉ có một dòng tên là driver

![Tab Executors chỉ có đúng một dòng và cột Executor ID ghi là driver: hãy nhìn cột Cores và cột Storage Memory của dòng đó để thấy toàn bộ tài nguyên nằm trong một tiến trình duy nhất](images/20_spark_executors.png)

Học viên thường ngạc nhiên ở tab này: sơ đồ kiến trúc ở mục 1.2 vẽ **một Driver và ba Executor** tách rời nhau, nhưng màn hình thật chỉ có **đúng một dòng**, và tên của nó là `driver` chứ không phải `0`, `1`, `2`.

Đó không phải lỗi. Chế độ `local[*]` **gộp Driver và Executor vào cùng một tiến trình JVM**: tiến trình Python của notebook vừa lập kế hoạch, vừa chạy task, vừa giữ dữ liệu cache. Ký hiệu `*` quyết định số **luồng** chạy task bên trong tiến trình đó, chứ không tạo thêm tiến trình nào. Đây chính là điều đã nói ở mục 1.2 và ở cảnh báo mục 0.4, nay hiện ra thành một dòng bảng.

> **Điều này dạy học viên điều gì?** Rằng **mọi khái niệm phân vùng, stage, task, shuffle đều hoạt động thật** ngay cả khi chỉ có một JVM — chỉ có dữ liệu là không đi qua mạng. Vì thế `local[*]` đủ để học trọn API, và cũng vì thế mọi con số thời gian đo được ở D1–D7 **không phản ánh** hành vi của một cụm thật.
>
> **Trên cụm thật, tab này là nơi quan trọng nhất để phát hiện sự cố.** Ở đó có nhiều dòng, mỗi dòng là một executor. Hãy nhìn ba thứ: cột `Failed Tasks` khác 0 nghĩa là có executor đang chết đi sống lại; một dòng chuyển sang trạng thái `Dead` giữa chừng nghĩa là Spark vừa phải dựng lại các phân vùng đã mất **từ lineage** — đúng cơ chế chịu lỗi ở mục 1.3; và cột `GC Time` chiếm phần lớn `Task Time` nghĩa là executor thiếu RAM, cần tăng `spark.executor.memory` hoặc giảm lượng dữ liệu đang cache.

### Spark Master UI — cổng 8088, đừng nhầm với 4040

![Trang Spark Master at spark://d9aa048284ce:7077 ở cổng 8088: hãy nhìn số phiên bản 3.5.1 ngay cạnh tiêu đề, dòng Alive Workers 1, Cores in use 2 Total, Memory in use 2.0 GiB Total, và đặc biệt là Running Applications 0](images/13_spark_master.png)

Đây là giao diện của **cụm Spark standalone trong Docker**, hoàn toàn khác với trang 4040. Ba điều cần chỉ cho học viên:

1. **Số phiên bản `3.5.1` hiện ngay trên trang**, sát bên tiêu đề `Spark Master at spark://d9aa048284ce:7077`. Đây **chính là con số phải khớp với phiên bản PySpark trên máy** nếu muốn nối vào cụm bằng `.master("spark://localhost:7077")`. Hãy nhắc lại cảnh báo ở mục 0.4: lệch phiên bản cho ra `java.io.InvalidClassException`, hoặc treo vô hạn ở `Initial job has not accepted any resources`. Trang này là chỗ tra con số đó — không cần đoán.
2. **`Alive Workers: 1`, `Cores in use: 2 Total`, `Memory in use: 2.0 GiB Total`** là toàn bộ tài nguyên của cụm standalone. Hai lõi và 2 GiB — ít hơn hẳn máy thật. Đây là lý do D1–D7 **không** chạy trên cụm này.
3. **`Running Applications: 0` và `Completed Applications: 0`.** Con số 0 này là điểm dạy: buổi học chạy ở chế độ `local[*]`, **không nộp việc vào cụm này**. Cụm vẫn sống, vẫn có worker, nhưng chưa bao giờ nhận việc. Học viên nào thấy `Running Applications: 0` mà tưởng cụm hỏng là đang hiểu ngược.

| | **`localhost:4040`** | **`localhost:8088`** |
|:---|:---|:---|
| **Tên đúng** | Spark UI của **một ứng dụng** | Spark **Master** UI của cụm standalone |
| **Ai tạo ra** | `SparkSession` trong tiến trình Python của bạn | Container `spark-master` trong Docker |
| **Trả lời câu hỏi** | Job **của tôi** chạy thế nào, shuffle bao nhiêu, cache gì | Cụm có mấy worker, còn bao nhiêu lõi và RAM rảnh |
| **Vòng đời** | Chết ngay khi `spark.stop()` | Sống chừng nào container còn chạy |
| **Trong buổi này** | Dùng suốt D1–D7 | Chỉ để quan sát; D8 chạy `spark-submit` với `local[*]` **bên trong** container |

> **Một cái bẫy rất thật về con số cổng.** Buổi 04 xem job YARN ở `localhost:8089`, buổi này xem Spark Master ở `localhost:8088` — hai cổng **lệch nhau đúng một chữ số**. Gõ nhầm một số là mở ra giao diện của công nghệ khác hẳn rồi kết luận sai. Hãy tập thói quen **đọc tiêu đề trang trước khi đọc số liệu trên trang**: nếu dòng đầu ghi `Spark Master at spark://...` thì đó là cụm Spark, không phải YARN.

### Đối chiếu giao diện web: Buổi 04 và Buổi 05

Cùng một câu hỏi nghiệp vụ, hai hệ sinh thái trả lời ở hai chỗ khác nhau:

| **Câu hỏi nghiệp vụ** | **Buổi 04 — Hadoop** | **Buổi 05 — Spark** |
|:---|:---|:---|
| Dữ liệu của tôi nằm ở đâu, chia thành mấy khối? | NameNode `9870` — `transactions.csv` 19.352.039 byte, 3 khối | Không có tab tương ứng; Spark **không** quản lý lưu trữ, vẫn xem ở `9870` |
| Việc của tôi đang chạy tới đâu? | ResourceManager `8089` | Spark UI `4040`, tab **Jobs** |
| Việc bị cắt thành mấy đoạn, shuffle bao nhiêu byte? | ResourceManager `8089`, xem counter của job | Spark UI `4040`, tab **Stages** |
| Job đã chạy xong hôm qua mất bao lâu? | JobHistory `19888` — **giữ lại sau khi job kết thúc** | `4040` chết theo phiên; cần dựng Spark History Server riêng |
| Cụm còn bao nhiêu tài nguyên rảnh? | ResourceManager `8089` | Spark Master `8088` |
| Cái gì đang nằm trong RAM? | Không có — MapReduce không giữ gì trong RAM giữa các job | Spark UI `4040`, tab **Storage** |
| Kế hoạch truy vấn của tôi trông thế nào? | Không có — MapReduce không có bộ tối ưu | Spark UI `4040`, tab **SQL / DataFrame** |

> **Hai điều bảng này dạy học viên.**
>
> Thứ nhất, **Spark thay thế tầng tính toán chứ không thay thế tầng lưu trữ**: cột giữa vẫn còn `9870` ở dòng đầu tiên, vì dữ liệu của D8 vẫn nằm nguyên trên HDFS. Đây đúng là kết luận sẽ chốt ở cuối phần thực hành.
>
> Thứ hai, và đây là khác biệt vận hành khiến nhiều người mới trả giá: **`19888` giữ lịch sử job vĩnh viễn, còn `4040` thì không.** Hadoop tách sẵn máy chủ lịch sử ra khỏi job; Spark thì gắn giao diện vào chính ứng dụng, nên ứng dụng kết thúc là bằng chứng biến mất. Trên hệ thống chạy thật, việc bật `spark.eventLog.enabled` cùng một Spark History Server là **bắt buộc** — nếu không, job hỏng lúc 2 giờ sáng sẽ không để lại gì cho bạn điều tra vào sáng hôm sau.


---

# 2. PHẦN THỰC HÀNH

## 2.1. Sơ đồ luồng toàn bài

```text
   lab4/outputs/lab4_timings.json          data/raw/  (y hệt Buổi 04)
        (thời gian MapReduce)          wordcount_corpus.txt · transactions.csv
                  │                                    │
                  │                                    ▼
                  │                         D1  SparkSession local[*]
                  │                                    │
                  │              ┌─────────────────────┼─────────────────────┐
                  │              ▼                     ▼                     ▼
                  │        D2  RDD API           D3  DataFrame API    D4  Spark SQL
                  │        flatMap/map/          groupBy/agg          createTempView
                  │        reduceByKey                                 + SELECT
                  │              │                     │                     │
                  │              └─────────────────────┼─────────────────────┘
                  │                                    ▼
                  │                    KẾT QUẢ PHẢI KHỚP BUỔI 04 TUYỆT ĐỐI
                  │                    373 từ · 206.744.802.000 VND
                  │                                    │
                  │              D5  explain() — đếm Exchange, hiểu stage
                  │              D6  cache() — đo lặp 8 vòng, có/không cache
                  │                                    │
                  └────────────────►  D7  BẢNG SO SÁNH ◄────────────────
                                                       │
                                       D8  Spark TRONG container, đọc HDFS
                                           — so sánh CÔNG BẰNG với Buổi 04
                                                       │
                                                       ▼
                                        lab5/outputs/lab5_ket_qua.json
```

## 2.2. Bài toán nghiệp vụ

> *"Hai báo cáo của Buổi 04 chạy mất 30 giây mỗi cái, và ban giám đốc muốn xem chúng theo yêu cầu chứ không phải chờ lịch chạy đêm. Hãy chuyển sang Apache Spark, chứng minh kết quả không đổi một chữ số, đo lại thời gian, và bổ sung ba báo cáo mới mà MapReduce làm rất cực: top sản phẩm, doanh thu theo tháng, và doanh thu theo phương thức thanh toán."*

**Đáp án chuẩn — phải khớp tuyệt đối với Buổi 04:**

| **WordCount** | **Giá trị** |
|:---|---:|
| Tổng số từ | 1.144 |
| Số từ khác nhau | **373** |
| `một` | 43 |
| `liệu` · `dữ` | 27 · 27 |

| **category** | **số giao dịch** | **doanh thu (VND)** | **TB/đơn** |
|:---|---:|---:|---:|
| Gia dụng | 32.231 | 58.925.123.000 | 1.828.213 |
| Điện tử | 36.296 | 57.872.658.000 | 1.594.464 |
| Thời trang | 27.778 | 47.277.119.000 | 1.701.963 |
| Thực phẩm | 59.856 | 18.555.159.000 | 309.997 |
| Mỹ phẩm | 19.839 | 17.421.590.000 | 878.149 |
| Sách | 24.000 | 6.693.153.000 | 278.881 |
| **Tổng** | **200.000** | **206.744.802.000** | |

## 2.3. Các bước thực hiện

### D1. Khởi tạo SparkSession *(4 phút)*

**Nhiệm vụ:** Tạo `SparkSession`, đọc dữ liệu, quan sát phân vùng, mở Spark UI.

**Các lệnh cần chạy:**

```python
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("Buoi05_Spark")
         .master("local[*]")                          # mỗi lõi CPU là một "máy"
         .config("spark.sql.shuffle.partitions", "8") # mặc định 200 — quá nhiều cho máy cá nhân
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
print("Phiên bản Spark     :", spark.version)
print("Chế độ chạy         :", sc.master)
print("Số lõi khả dụng     :", sc.defaultParallelism)
print("Spark UI            : http://localhost:4040")
```

**Kết quả thật sẽ thu được:**

```text
Phiên bản Spark     : 4.2.0
Chế độ chạy         : local[*]
Số lõi khả dụng     : 12
Thời gian khởi tạo  : khoảng 4 giây
```

> **Vì sao đổi `spark.sql.shuffle.partitions` xuống 8?** Mặc định Spark tạo **200 phân vùng** sau mỗi lần shuffle — con số hợp lý cho một cụm hàng trăm lõi, nhưng trên laptop nó tạo 200 task tí hon mà chi phí lập lịch còn lớn hơn công việc thật. Quy tắc chung là **2–4 phân vùng cho mỗi lõi** (máy 12 lõi thì 24–48). Bài này **cố ý đặt thấp hơn quy tắc**, chỉ **8**, vì dữ liệu chỉ 18,5 MB và phép `groupBy` chỉ sinh **6 nhóm** — đặt 24 phân vùng thì phần lớn sẽ rỗng. Quy tắc chung phải nhường chỗ cho đặc điểm dữ liệu thật.

> **Việc bắt buộc làm trên trình duyệt: mở http://localhost:4040 NGAY BÂY GIỜ.** Lúc này chưa có job nào. Từ D2 trở đi, mỗi action sẽ đẻ ra một dòng ở tab **Jobs** — đó là cách nhìn thấy lazy evaluation bằng mắt.
>
> **Lưu ý sống còn:** cổng 4040 **chỉ tồn tại khi SparkSession còn sống**. Gọi `spark.stop()` là trang đó chết ngay. Nếu 4040 bị chiếm, Spark tự nhảy sang 4041, 4042...

> **Hai cái bẫy khi đọc CSV:**
>
> 1. **`inferSchema=True` đọc tệp HAI LẦN** — một lần đoán kiểu, một lần đọc thật. Với dữ liệu lớn hãy khai báo lược đồ tường minh bằng `StructType`, hoặc dùng Parquet (đã có sẵn lược đồ bên trong).
> 2. **Số phân vùng của một tệp CSV do Spark tự quyết** theo kích thước tệp và số lõi — không liên quan gì đến **khối HDFS** của Buổi 04. Ở đây `transactions.csv` cho **5 phân vùng**, trong khi trên HDFS nó có **3 khối**.

---

### D2. RDD API — WordCount *(4 phút)*

**Nhiệm vụ:** Viết lại **đúng thuật toán MapReduce của Buổi 04** bằng RDD API, và đối chiếu từng con số.

**Các lệnh cần chạy:**

```python
# Quy tắc chuẩn hóa PHẢI GIỐNG HỆT Buổi 04, nếu không con số sẽ lệch
DAU_CAU  = str.maketrans({c: " " for c in '.,;:!?"()[]'})
HA_ASCII = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")

dong = sc.textFile(str(CORPUS_PATH))         # ①  giống  hdfs dfs -cat
print("Số phân vùng:", dong.getNumPartitions())

wc = (dong
      .flatMap(lambda d: d.translate(DAU_CAU).translate(HA_ASCII).split())  # ② = mapper.py
      .map(lambda tu: (tu, 1))                                             # ③ phát <khóa, 1>
      .reduceByKey(lambda a, b: a + b))                                    # ④ = reducer.py

print("Số từ khác nhau:", wc.count())        # ACTION đầu tiên — mọi thứ chạy ở đây
print("Tổng số từ     :", wc.map(lambda x: x[1]).sum())
for tu, sl in wc.takeOrdered(10, key=lambda x: (-x[1], x[0])):
    print(f"   {tu:<12}{sl:>5}")
```

**Kết quả thật sẽ thu được:**

```text
Số phân vùng    : 2
Số từ khác nhau : 373        ◄── TRÙNG KHỚP Buổi 04
Tổng số từ      : 1144       ◄── TRÙNG KHỚP Buổi 04
   một            43
   dữ             27
   liệu           27
   của            26
   là             20

Thời gian: khoảng 1,6 giây  (Buổi 04: ~30 giây)
```

**Đối chiếu từng dòng với Buổi 04 — đây là điểm dạy của D2:**

| **Buổi 04** | **Buổi 05** |
|:---|:---|
| `hdfs dfs -cat corpus.txt` | `sc.textFile(...)` |
| `wc_mapper.py`: tách dòng thành nhiều từ | `.flatMap(...)` |
| `wc_mapper.py`: `print(f"{tu}\t1")` | `.map(lambda tu: (tu, 1))` |
| `sort` (Shuffle & Sort của Hadoop) | ngầm bên trong `reduceByKey` |
| `wc_reducer.py`: cộng dồn theo khóa | `.reduceByKey(lambda a, b: a + b)` |
| Ba chương trình, hai tệp trung gian trên đĩa | **Một chuỗi bốn phép, không chạm đĩa** |

> **`reduceByKey` và `groupByKey` — câu hỏi phỏng vấn kinh điển, và đây là câu trả lời:**
>
> ```python
> rdd.reduceByKey(lambda a, b: a + b)      # ĐÚNG
> rdd.groupByKey().mapValues(sum)          # SAI — cùng kết quả, chậm hơn nhiều
> ```
>
> `reduceByKey` **cộng cục bộ trên từng phân vùng TRƯỚC** rồi mới shuffle — chỉ 373 cặp đi qua mạng.
> `groupByKey` **shuffle toàn bộ 1.144 cặp thô** rồi mới cộng.
>
> Với dữ liệu thật, chênh lệch này là hàng chục lần. Phép cộng cục bộ đó chính là cái mà Buổi 04 gọi là **combiner** — Spark làm tự động, MapReduce bắt bạn khai báo.

> **Vì sao `sc.textFile()` cho 2 phân vùng dù tệp chỉ 7 KB?** Vì `defaultMinPartitions` của Spark là **2**. Đây là lời nhắc: **số phân vùng của Spark không liên quan đến số khối HDFS**. Bạn điều khiển nó bằng tham số thứ hai của `textFile`, hoặc bằng `repartition()` / `coalesce()`.

---

#### Nhìn thấy shuffle bằng mắt trên tab Stages

Đoạn mã trên vừa sinh thêm vài dòng ở `http://localhost:4040`. Hãy vào tab **Stages** và nhấn vào stage đầu tiên của WordCount.

![Chi tiết Stage 0 của WordCount trên Spark UI — hãy nhìn tiêu đề "Summary Metrics for 2 Completed Tasks" và tiêu đề bảng "Tasks (2)" (đúng 2 task), rồi đọc dòng "Shuffle Write Size / Records: 4.4 KiB / 20" ở đầu trang (4,4 KiB chính là 4.545 byte), sau đó đối chiếu với Shuffle Read của Stage 1 ngay sau đó](images/16_spark_stage_wordcount.png)

Job WordCount tách thành **đúng hai stage**, và ranh giới nằm chính xác tại `reduceByKey`. Đây là hình ảnh cụ thể của câu "một lần shuffle cắt job thành hai stage" mà D5 sẽ nói lại bằng chữ trong `explain()`.

| **Điều cần nhìn** | **Stage 0** — `textFile` → `flatMap` → `map` | **Stage 1** — `reduceByKey` → `count` |
|:---|:---|:---|
| Việc phải làm | Đọc tệp, tách từ, phát cặp `<từ, 1>`, cộng cục bộ | Nhận dữ liệu qua mạng, cộng nốt theo khóa |
| Số task | **2** — đúng bằng số phân vùng mà `sc.textFile()` vừa báo | Do số phân vùng sau shuffle quyết định |
| Shuffle Write | **4.545 byte** | trống |
| Shuffle Read | trống | **4.545 byte** |

Hai con số **4.545 byte** bằng nhau tuyệt đối, và đó mới là điểm dạy: **không một byte nào rơi rớt trong shuffle**. Buổi 04 chứng minh đúng điều này bằng bộ đếm `Reduce input records = Map output records = 1.144`. Cùng một sự thật, chỉ khác chỗ đọc: Hadoop cho bạn **số bản ghi** trong nhật ký job, Spark cho bạn **số byte** ngay trên giao diện web.

> **Bài thực hành nhỏ, làm ngay tại lớp (2 phút).** Sửa `.reduceByKey(lambda a, b: a + b)` thành `.groupByKey().mapValues(sum)`, chạy lại, rồi mở tab **Stages** và so **cột `Shuffle Write` của hai lần chạy**. Con số sẽ **tăng rõ rệt**, vì `groupByKey` đẩy toàn bộ 1.144 cặp thô qua mạng thay vì 373 cặp đã cộng cục bộ.
>
> **Vì sao phải so cột đó chứ không bấm đồng hồ?** Vì trên 7 KB dữ liệu, chênh lệch thời gian bị nhiễu đo nuốt mất — đôi khi `groupByKey` còn ra nhanh hơn. Khối lượng đi qua shuffle thì không nói dối. Chọn đúng đại lượng để đo là một kỹ năng nghề, và đây là chỗ rèn nó.


---

### D3. DataFrame API — Doanh thu theo ngành hàng *(5 phút)* — **Trọng tâm**

**Nhiệm vụ:** Chạy lại bài toán D7 của Buổi 04 bằng DataFrame API.

**Các lệnh cần chạy:**

```python
df = spark.read.csv(str(TX_PATH), header=True, inferSchema=True)
df.printSchema()
print("Số dòng     :", df.count())
print("Số phân vùng:", df.rdd.getNumPartitions())

doanh_thu = (df
    .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.count("*").alias("so_giao_dich"),
         F.sum("revenue").alias("doanh_thu"),
         F.round(F.avg("revenue")).cast("long").alias("tb_don"))
    .orderBy(F.desc("doanh_thu")))
doanh_thu.show(truncate=False)
```

**Kết quả thật sẽ thu được:**

```text
+----------+------------+-----------+-------+
|category  |so_giao_dich|doanh_thu  |tb_don |
+----------+------------+-----------+-------+
|Gia dụng  |32231       |58925123000|1828213|
|Điện tử   |36296       |57872658000|1594464|
|Thời trang|27778       |47277119000|1701963|
|Thực phẩm |59856       |18555159000|309997 |
|Mỹ phẩm   |19839       |17421590000|878149 |
|Sách      |24000       |6693153000 |278881 |
+----------+------------+-----------+-------+

Thời gian groupBy: khoảng 1,3 giây   (Buổi 04: ~30 giây)
```

**Ba điều bắt buộc rút ra:**

1. **Sáu con số trùng khớp tuyệt đối với Buổi 04** — kể cả cột `tb_don` mà Buổi 04 phải tự viết trong reducer. Cùng dữ liệu, cùng phép tính, hai công nghệ khác hẳn nhau, một kết quả.

2. **Bốn dòng Python thay cho hai chương trình + một job.** Buổi 04 cần `tx_mapper.sh`, `tx_reducer.sh`, một lệnh `hadoop jar` với sáu tham số, và một thư mục đầu ra phải xóa trước. Ở đây là một biểu thức.

3. **`avg` làm được ngay.** Buổi 04 phải giữ **cả tổng lẫn số đếm** trong reducer rồi chia thủ công, vì `AVG` không cộng dồn được. Spark lo việc đó — nhưng **bản chất bên dưới vẫn thế**, `explain()` ở D5 sẽ cho thấy `partial_avg` rồi mới tới `avg`.

> **Vì sao `df.count()` chạy chậm hơn bạn tưởng?** Vì nó là một **action**: Spark đọc và phân tích lại toàn bộ CSV. Gọi `count()` ba lần là đọc tệp ba lần. Đây chính là vấn đề mà D6 giải bằng `cache()`.

---

#### Tab SQL / DataFrame — kế hoạch truy vấn vẽ thành đồ thị

Ngay sau `doanh_thu.show()`, mở `http://localhost:4040`, vào tab **SQL / DataFrame** và nhấn vào dòng truy vấn vừa chạy. Spark vẽ lại toàn bộ kế hoạch thực thi thành một đồ thị đọc từ trên xuống.

![Trang Details for Query 2 — hãy nhìn ba con số ghi trên các mũi tên nối giữa các nút: 200,000 đi vào, 30 đi ra khỏi HashAggregate cục bộ, và 6 đi ra khỏi HashAggregate cuối](images/18_spark_sql_plan.png)

**Ba con số trên các mũi tên là thứ đáng giá nhất của cả trang này: 200.000 → 30 → 6.**

1. **200.000** — số dòng `Scan csv` đọc lên, đúng bằng số giao dịch trong `transactions.csv`.
2. **30** — số dòng còn lại **trước khi** tới `Exchange`. Con số này không ngẫu nhiên: **30 = 6 ngành hàng × 5 phân vùng**. Mỗi phân vùng đã tự gom phần dữ liệu của mình xuống còn 6 dòng.
3. **6** — kết quả cuối, sau khi `HashAggregate` thứ hai cộng nốt các kết quả cục bộ theo ngành hàng.

Điều học viên phải rút ra: **chỉ 30 dòng đi qua mạng thay vì 200.000 dòng** — giảm hơn 6.000 lần. Phép gom cục bộ đó **chính xác là combiner của Buổi 04**. Khác biệt duy nhất nằm ở chỗ: Buổi 04 có bộ đếm `Combine input records` bằng **0** vì combiner không được bật, còn Spark làm việc này **tự động, mặc định, không cần khai báo một dòng nào**.

| **Nút trên đồ thị** | **Dòng tương ứng trong `explain()` ở D5** | **Khái niệm lý thuyết** |
|:---|:---|:---|
| `Scan csv` | `FileScan csv [category,quantity,unit_price]` | Đọc dữ liệu, đã cắt bớt cột |
| `Project` | `Project [category, (quantity * unit_price) AS revenue]` | Phần thân của mapper Buổi 04 |
| `HashAggregate` (phía trên) | `HashAggregate(... [partial_count, partial_sum, partial_avg])` | **Combiner** — cộng cục bộ trước shuffle |
| `Exchange` | `Exchange hashpartitioning(category, 8)` | **Shuffle & Sort**, ranh giới giữa hai stage |
| `AQEShuffleRead` | `AQEShuffleRead coalesced` trong `== Final Plan ==` | AQE gộp các phân vùng nhỏ sau shuffle |
| `HashAggregate` (phía dưới) | `HashAggregate(... [count, sum, avg])` | Reducer — cộng các kết quả cục bộ |
| `TakeOrderedAndProject` | `Sort` + `Exchange rangepartitioning` khi không có `LIMIT` | Xếp hạng toàn cục — thứ MapReduce cần thêm một job |
| `AdaptiveSparkPlan` | dòng ngoài cùng của kế hoạch | Cho phép sửa kế hoạch giữa chừng |

> **`AQEShuffleRead` và `AdaptiveSparkPlan` nói gì?** `AdaptiveSparkPlan` là lớp vỏ ngoài cùng, cho phép Spark **sửa lại kế hoạch giữa chừng** dựa trên kích thước dữ liệu **thật** đo được sau mỗi shuffle. `AQEShuffleRead` là kết quả cụ thể của quyền đó: thấy 30 dòng mà chia thành 8 phân vùng là quá vụn, AQE **gộp chúng lại** để khỏi sinh ra những task tí hon. Hãy nối ngay với ghi chú `== Final Plan ==` / `== Initial Plan ==` ở D5: đồ thị này vẽ **kế hoạch cuối**, tức kế hoạch đã được AQE sửa.

> **Vì sao đồ thị chỉ có MỘT `Exchange` trong khi `doanh_thu.explain()` ở D5 có HAI?** Vì `show()` ngầm thêm `LIMIT 20`. Thấy có `LIMIT`, Catalyst thay cặp `Exchange rangepartitioning` + `Sort` bằng một nút `TakeOrderedAndProject` — mỗi phân vùng chỉ giữ vài dòng đầu rồi gộp, khỏi shuffle lần hai. Cùng một biểu thức DataFrame, hai action khác nhau, hai kế hoạch khác nhau: đó mới là nghĩa thật của câu "Catalyst tối ưu theo ngữ cảnh".

> **Một chi tiết dễ gây hoang mang, nói trước cho lớp:** dòng đầu trang ghi `Duration 9 s` và `Succeeded Jobs 3, 4`. Con số 9 giây là thời gian của **cả truy vấn** kể từ lúc nộp, gồm cả phần đọc tệp; nó không mâu thuẫn với 1,3 giây mà `time.perf_counter()` đo riêng cho phép `groupBy`. Biết mình đang đọc đồng hồ nào là một phần của kỹ năng đọc giao diện.

Quay lại danh sách để thấy điều quan trọng cuối cùng của tab này:

![Tab SQL / DataFrame với 15 truy vấn đã hoàn thành — hãy để ý các biểu thức DataFrame của D3 và ba câu spark.sql() của D4 nằm chung trong một danh sách duy nhất](images/17_spark_sql_list.png)

> **Điểm dạy của tấm ảnh này:** biểu thức DataFrame của D3 và ba câu `spark.sql()` của D4 **nằm chung một danh sách**, không phải hai danh sách riêng. Lý do rất đơn giản và rất quan trọng: cả hai **biên dịch về cùng một kế hoạch Catalyst**. Spark UI không có chỗ nào phân biệt "truy vấn viết bằng SQL" với "truy vấn viết bằng Python", vì tới tầng này chúng đã là **một thứ**. Đây là bằng chứng bằng hình cho khẳng định ở D4: chọn SQL hay DataFrame API là chọn theo người đọc, không phải chọn theo hiệu năng.


---

### D4. Spark SQL — ba báo cáo mới *(5 phút)*

**Nhiệm vụ:** Đăng ký DataFrame thành bảng tạm, rồi viết **đúng cú pháp SQL của Buổi 03** trên dữ liệu phân tán.

**Các lệnh cần chạy:**

```python
df.withColumn("revenue", F.col("quantity") * F.col("unit_price")) \
  .createOrReplaceTempView("tx")

spark.sql("""
    SELECT product, SUM(revenue) AS doanh_thu
    FROM tx GROUP BY product ORDER BY doanh_thu DESC LIMIT 5
""").show(truncate=False)

spark.sql("""
    SELECT date_format(ts, 'yyyy-MM') AS thang,
           COUNT(*) AS so_giao_dich, SUM(revenue) AS doanh_thu
    FROM tx GROUP BY thang ORDER BY thang
""").show(truncate=False)

spark.sql("""
    SELECT payment_method, COUNT(*) AS so_giao_dich, SUM(revenue) AS doanh_thu
    FROM tx GROUP BY payment_method ORDER BY doanh_thu DESC
""").show(truncate=False)
```

**Kết quả thật sẽ thu được:**

*Top 5 sản phẩm theo doanh thu:*

| **product** | **doanh_thu** |
|:---|---:|
| Quạt điều hòa | 26.796.034.000 |
| Bàn phím cơ | 18.430.372.000 |
| Ổ cứng SSD 512GB | 16.763.176.000 |
| Túi xách | 15.553.674.000 |
| Nồi cơm điện | 13.909.505.000 |

*Doanh thu theo tháng:*

| **thang** | **so_giao_dich** | **doanh_thu** |
|:---|---:|---:|
| 2025-01 | 37.660 | 38.781.757.000 |
| 2025-02 | 34.284 | 35.808.097.000 |
| 2025-03 | 38.019 | 39.171.277.000 |
| 2025-04 | 36.753 | 38.139.527.000 |
| 2025-05 | 38.188 | 39.321.164.000 |
| 2025-06 | 15.096 | 15.522.980.000 |

*Doanh thu theo phương thức thanh toán:*

| **payment_method** | **so_giao_dich** | **doanh_thu** |
|:---|---:|---:|
| TIEN_MAT | 67.770 | 69.446.993.000 |
| THE | 60.060 | 62.621.625.000 |
| VI_DIEN_TU | 52.059 | 53.813.087.000 |
| CHUYEN_KHOAN | 20.111 | 20.863.097.000 |

**Ba điểm dạy từ ba bảng này:**

1. **Tháng 6 chỉ có 15.096 giao dịch, chưa bằng một nửa các tháng khác.** Đây **không** phải sụt giảm kinh doanh — bộ dữ liệu chỉ ghi tới giữa tháng 6. **Luôn kiểm tra biên của dữ liệu trước khi kết luận về xu hướng**; đây là lỗi báo cáo phổ biến nhất trong thực tế.

2. **`ORDER BY ... LIMIT 5` là thứ MapReduce làm rất cực.** Buổi 04, Phụ lục A bài 2 đã chỉ ra: reducer chỉ nhìn thấy **một khóa tại một thời điểm** nên không xếp hạng toàn cục được — phải chạy job thứ hai. Spark làm trong một câu.

3. **Ba câu SQL này chạy được **nguyên văn** trên PostgreSQL của Buổi 03.** Cùng ngôn ngữ, khác động cơ: một bên chạy trên một máy chủ, một bên chạy phân tán trên cả cụm. Đó là giá trị lớn nhất của Spark SQL — **kỹ năng SQL của bạn chuyển thẳng sang Big Data**.

> **DataFrame API hay Spark SQL — chọn cái nào?** Cả hai **biên dịch về cùng một kế hoạch Catalyst**, hiệu năng **y hệt nhau**. Chọn theo người đọc: SQL dễ cho người làm nghiệp vụ, DataFrame API dễ kiểm thử và ghép thành hàm trong Python. Trộn cả hai trong một chương trình là hoàn toàn bình thường.

---

### D5. Lazy evaluation và `explain()` *(5 phút)*

**Nhiệm vụ:** Chứng minh bằng số rằng transformation **không** tính gì, rồi đọc kế hoạch vật lý để đếm shuffle.

**Các lệnh cần chạy:**

```python
# ① Transformation KHÔNG tính gì
t0 = time.perf_counter()
tam = (df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))
         .filter(F.col("quantity") >= 3)
         .groupBy("city").agg(F.sum("revenue").alias("rev")))
print(f"Dựng chuỗi 3 biến đổi : {(time.perf_counter()-t0)*1000:.1f} ms")

# ② Action mới kích hoạt
t0 = time.perf_counter()
tam.collect()
print(f"Gọi .collect()        : {(time.perf_counter()-t0)*1000:.1f} ms")

# ③ NARROW — không có Exchange
print("\n=== KẾ HOẠCH NARROW (không shuffle) ===")
df.filter(F.col("quantity") > 3).select("category", "quantity").explain()

# ④ WIDE — có Exchange
print("\n=== KẾ HOẠCH WIDE (có shuffle) ===")
doanh_thu.explain()

# ⑤ Đếm shuffle cho chắc chắn
def dem_exchange(kh):
    # Nếu AQE đã chạy xong, explain() in RA HAI kế hoạch (Final + Initial).
    # Chỉ đếm phần Initial Plan để con số không bị nhân đôi.
    if "== Initial Plan ==" in kh:
        kh = kh.split("== Initial Plan ==", 1)[1]
    return sum(1 for d in kh.split("\n") if d.strip().removeprefix("+- ").startswith("Exchange"))
```

**Kết quả thật sẽ thu được:**

```text
Dựng chuỗi 3 biến đổi :   3,2 ms      ◄── gần như bằng 0: CHƯA TÍNH GÌ
Gọi .collect()        : 480,5 ms      ◄── TẤT CẢ chạy ở đây

=== KẾ HOẠCH NARROW ===
*(1) Filter (isnotnull(quantity) AND (quantity > 3))
+- FileScan csv [category,quantity] ... PushedFilters: [GreaterThan(quantity,3)]
        │                    │                    │
        │                    │                    └─ bộ lọc bị ĐẨY XUỐNG tận lúc đọc tệp
        │                    └─ chỉ đọc 2 trên 10 cột — CẮT BỚT CỘT
        └─ KHÔNG có dòng Exchange nào -> một stage duy nhất

=== KẾ HOẠCH WIDE ===
Sort [doanh_thu DESC]
+- Exchange rangepartitioning(doanh_thu DESC, 8)      ◄── SHUFFLE thứ hai (do orderBy)
   +- HashAggregate(keys=[category], functions=[count, sum, avg])
      +- Exchange hashpartitioning(category, 8)       ◄── SHUFFLE thứ nhất (do groupBy)
         +- HashAggregate(keys=[category], functions=[partial_count, partial_sum, partial_avg])
            +- Project [category, (quantity * unit_price) AS revenue]
               +- FileScan csv [category,quantity,unit_price]
```

**Bốn điều bắt buộc rút ra — đây là phần đắt giá nhất của buổi học:**

1. **Ba biến đổi mất 3 mili giây, một action mất 480 mili giây.** Lazy evaluation không phải lý thuyết — nó đo được.

2. **`PushedFilters` là Catalyst đang làm việc cho bạn.** Bạn viết "đọc tệp rồi lọc", Catalyst đổi thành "lọc ngay lúc đọc". Và `ReadSchema` chỉ có 2 cột dù tệp có 10 — nó **không đọc** 8 cột kia.

3. **Đếm `Exchange` là đếm shuffle.** Kế hoạch narrow có **0**, kế hoạch wide có **2** — một do `groupBy`, một do `orderBy`. Hai `Exchange` = ba stage. Mở tab **Stages** ở `localhost:4040` để nhìn thấy đúng ba khối đó.

4. **`partial_count` / `partial_sum` xuất hiện TRƯỚC `Exchange`** — đây chính là **combiner** của Buổi 04! Spark cộng cục bộ trên từng phân vùng trước, rồi mới gửi kết quả cục bộ qua mạng. Cùng ý tưởng `reduceByKey` ở D2, và cùng ý tưởng combiner mà MapReduce bắt bạn tự khai báo.

> **Một chi tiết thật của Spark 3+ mà lớp sẽ đụng ngay ở bước này — hãy nói trước.**
>
> Nếu DataFrame **đã từng chạy một action**, `explain()` in ra **hai** kế hoạch chứ không phải một:
>
> - `== Final Plan ==` — kế hoạch **sau khi** AQE (*Adaptive Query Execution*) chỉnh lại dựa trên kích thước dữ liệu **thật** đo được lúc chạy. Trong đó có `AQEShuffleRead coalesced`: AQE đã **gộp các phân vùng nhỏ** sau shuffle — tức là nó **tự sửa** con số `spark.sql.shuffle.partitions = 8` mà bạn đặt ở D1.
> - `== Initial Plan ==` — kế hoạch Catalyst dựng **trước khi** chạy.
>
> Hệ quả thực tế: đếm chuỗi `"Exchange"` trên toàn bộ đầu ra sẽ ra **4** thay vì **2**. Đó là lý do hàm `dem_exchange` ở trên phải cắt lấy phần `Initial Plan`. Học viên nào đếm ra 4 thì **không sai** — họ chỉ đang đếm cùng một shuffle hai lần.

> **Kỹ năng cần rèn suốt phần đời còn lại làm Spark:** đọc `explain()` **trước khi** chạy job lớn. Một `Exchange` thừa trên 1 TB dữ liệu là hàng chục phút và hàng trăm nghìn đồng tiền cụm. Nhìn kế hoạch mất 5 giây; chạy sai mất cả buổi chiều.

---

### D6. `cache()` — đo lợi ích bằng số *(4 phút)*

**Nhiệm vụ:** Chạy một khối lượng công việc **lặp** hai lần — không cache và có cache — rồi so.

**Các lệnh cần chạy:**

```python
co_so = (df.withColumn("revenue", F.col("quantity") * F.col("unit_price"))
           .filter(F.col("quantity") >= 2))
VONG = 8

def do_thoi_gian(d, nhan):
    t0 = time.perf_counter()
    for _ in range(VONG):
        d.groupBy("category").agg(F.sum("revenue"), F.count("*")).collect()
    dt = time.perf_counter() - t0
    print(f"{nhan:<24}{dt:7.2f} s   ({dt/VONG*1000:6.0f} ms/vòng)")
    return dt

t_khong_cache = do_thoi_gian(co_so, "KHÔNG cache")

co_so.cache()
co_so.count()                       # action đầu tiên mới thực sự nạp vào bộ nhớ
t_co_cache = do_thoi_gian(co_so, "CÓ cache")

print(f"Nhanh hơn {t_khong_cache/t_co_cache:.1f} lần")
print("StorageLevel:", co_so.storageLevel)
co_so.unpersist()
```

**Kết quả thật sẽ thu được:**

```text
KHÔNG cache                1.63 s   (   204 ms/vòng)
CÓ cache                   0.53 s   (    66 ms/vòng)
Nhanh hơn 3.1 lần
StorageLevel: Disk Memory Deserialized 1x Replicated
```

> **Vì sao "chỉ" 3 lần chứ không phải 100 lần?** Vì `transactions.csv` chỉ 18,5 MB và chuỗi biến đổi rất ngắn — phần tiết kiệm được (đọc + phân tích CSV) không lớn. Khoảng cách rộng ra khi:
>
> - dữ liệu lớn hơn (đọc lại tệp đắt hơn nhiều),
> - chuỗi biến đổi dài hơn (phải tính lại nhiều bước hơn),
> - số vòng lặp nhiều hơn (thuật toán học máy lặp hàng chục vòng).
>
> **Hãy để học viên đổi `VONG = 8` thành `VONG = 30`** và xem khoảng cách nới ra thế nào. Đó là bằng chứng trực quan cho câu "Spark sinh ra vì thuật toán lặp".

> **`cache()` sau `.count()` mới có tác dụng.** `cache()` cũng **lười** — nó chỉ *đánh dấu* rằng "hãy giữ lại kết quả", chứ không tính gì. Phải có một **action** chạy qua thì dữ liệu mới thực sự vào bộ nhớ. Quên `count()` sau `cache()` là lỗi rất phổ biến khi đo hiệu năng: vòng lặp đầu tiên sẽ gánh luôn chi phí nạp cache và làm số đo sai.

> **Kiểm chứng bằng mắt:** mở tab **Storage** ở `localhost:4040` sau khi cache. Bạn sẽ thấy đúng một mục với số phân vùng đã nạp và dung lượng RAM đang chiếm. `unpersist()` xong thì mục đó biến mất.

---

#### Tab Storage — bằng chứng dữ liệu đang nằm trong bộ nhớ

Con số "nhanh hơn 3,1 lần" mới chỉ là đồng hồ. Bằng chứng thật nằm ở tab **Storage** của `http://localhost:4040` — hãy mở nó **sau `co_so.count()` nhưng trước `co_so.unpersist()`**.

![Tab Storage sau khi cache — hãy đọc lần lượt trên đúng một dòng RDD duy nhất: cột Storage Level, Cached Partitions 5, Fraction Cached 100.00%, Size in Memory 4.0 MiB và Size on Disk 0.0 B](images/19_spark_storage.png)

| **Cột** | **Giá trị đọc được** | **Điều nó chứng minh** |
|:---|:---|:---|
| Storage Level | `Disk Memory Deserialized 1x Replicated` | Khớp tuyệt đối với dòng `print(co_so.storageLevel)` trong notebook. `Disk Memory` = ưu tiên RAM, thiếu chỗ mới tràn xuống đĩa; `Deserialized` = giữ nguyên đối tượng, đọc nhanh nhưng tốn RAM; `1x` = không nhân bản |
| Cached Partitions | 5 | Đúng bằng số phân vùng của DataFrame ở D3 — cache làm việc theo **từng phân vùng**, không phải theo cả bảng |
| Fraction Cached | 100,00% | Toàn bộ dữ liệu đã vào bộ nhớ. Nếu **nhỏ hơn 100%** nghĩa là **không đủ RAM**: phần thiếu sẽ bị tính lại mỗi lần dùng, và số đo hiệu năng sẽ tệ đi mà không rõ nguyên nhân |
| Size in Memory | `4.0 MiB` | Nhỏ hơn hẳn tệp CSV 18,5 MB, vì trong bộ nhớ dữ liệu đã có kiểu rõ ràng và đã qua bộ lọc `quantity >= 2` |
| Size on Disk | `0.0 B` | **Chưa một phân vùng nào phải tràn xuống đĩa** — đúng nghĩa "xử lý trong bộ nhớ" |

Điều học viên phải rút ra: `cache()` không phải một lời hứa mơ hồ, nó là **năm con số kiểm chứng được**. Khi cần biện minh cho việc chiếm RAM của cụm, đây chính là trang bạn đưa ra.

> **Thí nghiệm hai bước chứng minh `cache()` cũng lười — làm ngay tại lớp.**
>
> 1. Chạy `co_so.cache()`, rồi mở tab **Storage** **trước khi** gọi `count()`. Trang **vẫn trống**, không có dòng nào.
> 2. Chạy `co_so.count()`, tải lại trang. Bây giờ dòng RDD ở trên mới xuất hiện, kèm đủ các cột.
>
> Đây là bằng chứng bằng mắt cho ghi chú phía trên: `cache()` chỉ **đánh dấu ý định**, phải có một **action** chạy qua thì dữ liệu mới thực sự vào bộ nhớ. Học viên nào từng đo hiệu năng sai vì quên `count()` sẽ nhớ trang trống này lâu hơn nhớ một câu giải thích.

> **Làm nốt bước cuối:** gọi `co_so.unpersist()` rồi tải lại tab **Storage** — dòng đó **biến mất**, RAM được trả lại. Trong một notebook chạy lâu, quên `unpersist()` những DataFrame không còn dùng là cách nhanh nhất để đẩy phiên Spark tới `OutOfMemoryError`.


---

### D7. So sánh hiệu năng — và cách so sánh cho **công bằng** *(4 phút)*

**Nhiệm vụ:** Đọc `lab4/outputs/lab4_timings.json`, lập bảng so sánh, và **chỉ ra chỗ so sánh đó chưa công bằng**.

**Các lệnh cần chạy:**

```python
import json
tm = json.loads((GOC / "lab4" / "outputs" / "lab4_timings.json").read_text(encoding="utf-8"))

print(f"{'Phép tính':<34}{'Buổi 04 (MR)':>14}{'Buổi 05 (Spark)':>17}{'Nhanh hơn':>12}")
print("-" * 77)
for ten, t_mr, t_sp in [("WordCount (7 KB)",           tm["yarn_wordcount_s"], t_d2),
                        ("Doanh thu (18,5 MB)",        tm["yarn_revenue_s"],   t_d3)]:
    print(f"{ten:<34}{t_mr:>12.1f} s{t_sp:>15.2f} s{t_mr/t_sp:>10.1f}x")
```

**Kết quả thật sẽ thu được:**

| **Phép tính** | **Buổi 04 (MapReduce/YARN)** | **Buổi 05 (Spark `local[*]`)** | **Nhanh hơn** |
|:---|---:|---:|---:|
| WordCount (7 KB) | 29,2 s | 1,62 s | **18×** |
| Doanh thu (18,5 MB) | 29,2 s | 1,28 s | **23×** |

> **DỪNG LẠI. Bảng trên KHÔNG công bằng, và việc nhận ra điều đó quan trọng hơn chính con số.**
>
> Hãy để học viên tự tìm ra ba chỗ khập khiễng:
>
> 1. **Khác phần cứng.** MapReduce chạy trong container Docker; Spark chạy thẳng trên máy thật. Trên máy Apple Silicon, container Hadoop còn chạy qua **lớp giả lập amd64** — chậm hơn đáng kể trước khi tính bất cứ điều gì khác.
> 2. **Khác thứ được tính vào.** Thời gian MapReduce gồm cả xin tài nguyên YARN và khởi động JVM; thời gian Spark **không** gồm 4 giây khởi tạo `SparkSession` vì phiên đã sẵn sàng từ D1.
> 3. **Khác quy mô dữ liệu so với hạ tầng.** 18,5 MB là quá nhỏ để bất kỳ hệ phân tán nào thể hiện đúng bản chất.
>
> **Bài học nghề nghiệp lớn nhất của buổi học:** một con số hiệu năng chỉ có nghĩa khi **mọi thứ khác được giữ nguyên**. Bảng trên vẫn hữu ích — nó cho thấy trải nghiệm thực tế của người dùng — nhưng gọi nó là "Spark nhanh hơn MapReduce 23 lần" thì **sai về phương pháp**.
>
> Vì vậy mới có D8.

---

### D8. So sánh **công bằng** — Spark chạy trong chính cụm Docker *(4 phút)*

**Nhiệm vụ:** Chạy đúng hai phép tính đó bằng Spark **bên trong container**, đọc dữ liệu **thẳng từ HDFS** — cùng phần cứng, cùng lớp giả lập, cùng nguồn dữ liệu với job MapReduce Buổi 04.

**Các lệnh cần chạy:**

```bash
docker cp lab5/spark_jobs/revenue_hdfs.py bigdata-spark-master:/tmp/
docker exec bigdata-spark-master \
    /opt/bitnami/spark/bin/spark-submit --master "local[*]" /tmp/revenue_hdfs.py
```

**Kết quả thật sẽ thu được:**

```text
================================================================
SPARK CHẠY TRONG CỤM DOCKER, ĐỌC THẲNG TỪ HDFS
================================================================
Khởi tạo SparkSession :   0.71 s
Doanh thu theo ngành  :   4.24 s   (Buổi 04 mất ~30 s)
WordCount             :   0.82 s   (Buổi 04 mất ~30 s)
----------------------------------------------------------------
  Gia dụng       32,231    58,925,123,000
  Điện tử        36,296    57,872,658,000
  Thời trang     27,778    47,277,119,000
  Thực phẩm      59,856    18,555,159,000
  Mỹ phẩm        19,839    17,421,590,000
  Sách           24,000     6,693,153,000
----------------------------------------------------------------
  Số từ khác nhau: 373   (kỳ vọng 373)
================================================================

Tổng thời gian spark-submit (kể cả khởi động JVM): 8,5 giây
```

**BẢNG SO SÁNH CÔNG BẰNG — cùng máy, cùng container, cùng dữ liệu trên HDFS:**

| **Phép tính** | **MapReduce trên YARN** | **Spark trong cụm** | **Nhanh hơn** |
|:---|---:|---:|---:|
| WordCount (7 KB, từ HDFS) | ~29 s | **0,8 s** | **~36×** |
| Doanh thu (18,5 MB, từ HDFS) | ~29 s | **4,2 s** | **~7×** |
| Tổng cả hai, kể cả khởi động | ~58 s | **8,5 s** | **~7×** |

**Ba điều bắt buộc rút ra:**

1. **Kết quả giống hệt tới từng chữ số** — 373 từ, 206.744.802.000 VND. Spark **không tính khác**; nó chỉ **không ghi đĩa giữa các giai đoạn**.

2. **Hai phép tính chạy trong MỘT phiên Spark.** Buổi 04 cần **hai job Hadoop riêng biệt**, mỗi job trả lại chi phí khởi động từ đầu. Spark khởi động **một lần** rồi làm cả hai — và đó là lý do vì sao càng nhiều bước liên tiếp, Spark càng thắng đậm.

3. **Chênh lệch không đồng đều: WordCount thắng 36×, doanh thu chỉ thắng 7×.** Vì WordCount xử lý 7 KB nên gần như toàn bộ 29 giây của MapReduce là **chi phí cố định** — bỏ được chi phí đó là thắng đậm. Còn với 18,5 MB, một phần thời gian là **tính toán thật**, mà tính toán thật thì Spark không thể làm biến mất.

> **Kết luận cần chốt của cả buổi học:**
>
> Spark nhanh hơn MapReduce **không phải vì thuật toán tốt hơn** — thuật toán y hệt: chia khóa, gom khóa, tổng hợp. Spark nhanh hơn vì **ba lý do rất cụ thể**:
>
> 1. **Không ghi dữ liệu trung gian xuống đĩa** giữa các giai đoạn.
> 2. **Một phiên làm nhiều việc**, thay vì mỗi job trả lại chi phí khởi động.
> 3. **Catalyst tối ưu lại kế hoạch** — đẩy bộ lọc xuống, cắt bớt cột, cộng cục bộ trước khi shuffle.
>
> Và Spark **không** thay thế Hadoop hoàn toàn: dữ liệu vẫn nằm trên **HDFS**, tài nguyên vẫn có thể do **YARN** cấp. Spark thay thế **MapReduce** — tầng tính toán — chứ không thay thế tầng lưu trữ. Đó là lý do hai buổi học này đi liền nhau.

---

#### Đối chiếu bằng giao diện web: Buổi 04 và Buổi 05 cạnh nhau

Hai buổi học vừa qua đưa cho bạn hai bộ giao diện web khác hẳn nhau. Nhưng nếu xếp chúng theo **câu hỏi cần trả lời** thay vì theo tên sản phẩm, chúng ứng với nhau gần như từng dòng một. Hãy giữ bảng này bên cạnh mỗi khi phải gỡ lỗi hiệu năng.

| **Câu hỏi** | **Buổi 04 mở ở đâu** | **Buổi 05 mở ở đâu** |
|:---|:---|:---|
| Dữ liệu nằm ở đâu, chia thành mấy phần? | `localhost:9870` → **Utilities → Browse the file system** (3 khối) | `getNumPartitions()` trong notebook (5 phân vùng); riêng D8 vẫn đọc chính `localhost:9870` |
| Cụm còn bao nhiêu máy làm việc sống? | `localhost:8089` → mục **Nodes** (NodeManager) | `localhost:8088` → **Alive Workers**; ở chế độ `local[*]` thì xem `localhost:4040` → tab **Executors** |
| Việc của tôi đang chạy tới đâu? | `localhost:8089` → thanh **Progress** của job | `localhost:4040` → tab **Jobs**, mỗi dòng là **một action** |
| Việc tách thành mấy đơn vị nhỏ nhất? | Số **Map task** và **Reduce task** của job | `localhost:4040` → tab **Stages**, cột **Tasks: Succeeded/Total** |
| Bao nhiêu dữ liệu đi qua shuffle? | Bộ đếm `Map output records` / `Reduce input records` trong nhật ký job | tab **Stages**, cột **Shuffle Read** / **Shuffle Write**, tính bằng byte |
| Dữ liệu có được gom lại trước khi shuffle không? | Bộ đếm `Combine input records` — bằng **0** vì combiner không bật | tab **SQL / DataFrame**, con số trên mũi tên ngay trước `Exchange`: 200.000 → **30** |
| Kế hoạch thực thi trông thế nào? | **Không có gì để xem** — bạn tự viết mapper và reducer | tab **SQL / DataFrame** → đồ thị kế hoạch, và `explain()` trong notebook |
| Dữ liệu có đang nằm trong bộ nhớ không? | Không có khái niệm này | tab **Storage** |
| Task nào chạy lệch giờ (data skew)? | So thời gian từng **attempt** ở `localhost:19888` | tab **Stages** → **Summary Metrics**, so cột Min / Median / Max |
| Job đã chạy xong xem lại ở đâu? | `localhost:19888` (JobHistory) giữ lịch sử | Không có sẵn — cổng 4040 **chết theo `SparkSession`**; muốn giữ lại phải bật Spark History Server |

Điều học viên phải rút ra từ bảng này: bạn **không** học hai bộ công cụ rời rạc. Bạn học **một danh sách câu hỏi chẩn đoán** và hai chỗ để tra câu trả lời.

> **Điều đáng mang theo ra khỏi hai buổi học:** hai hệ sinh thái khác nhau, hai bộ giao diện khác nhau, nhưng **cùng một tư duy chẩn đoán** — tìm **đơn vị công việc nhỏ nhất** (task), xem **nó chạy ở đâu** và **mất bao lâu**, rồi tìm **chỗ lệch**. Công cụ sẽ đổi theo từng năm; ba câu hỏi đó thì không. Ai trả lời được chúng trên một hệ phân tán bất kỳ sẽ gỡ được lỗi hiệu năng trên hệ tiếp theo.


## 2.4. Tiêu chí hoàn thành

```text
☐ Tạo được SparkSession local[*], mở được http://localhost:4040

☐ RDD WordCount ra ĐÚNG 373 từ khác nhau / 1.144 tổng số từ, khớp Buổi 04

☐ Giải thích được vì sao reduceByKey tốt hơn groupByKey

☐ DataFrame cho ra 6 ngành hàng khớp tuyệt đối, tổng 206.744.802.000 VND

☐ Viết được ít nhất 3 truy vấn Spark SQL và đọc đúng ý nghĩa kết quả

☐ Chỉ ra được vì sao tháng 6 chỉ có 15.096 giao dịch

☐ Chứng minh bằng số rằng transformation không tính gì, action mới tính

☐ Đếm đúng số dòng Exchange trong explain() và giải thích mỗi dòng do đâu

☐ Chỉ ra partial_count/partial_sum và nối được với khái niệm combiner của Buổi 04

☐ Đo được lợi ích của cache() và nêu đúng ba quy tắc dùng cache

☐ Lập được bảng so sánh với Buổi 04 VÀ chỉ ra được ba chỗ so sánh chưa công bằng

☐ Chỉ ra được trên Spark UI: mỗi dòng ở tab Jobs là một action; Shuffle Write của stage N
  bằng Shuffle Read của stage N+1; tab Storage có đúng một dòng sau khi cache

☐ Đọc được đồ thị kế hoạch ở tab SQL và giải thích được chuỗi 200.000 → 30 → 6

☐ Chạy được Spark trong container đọc HDFS, có bảng so sánh công bằng
```

---

# 3. TỔNG KẾT BUỔI HỌC

## 3.1. Sản phẩm phải nộp

| **\#** | **Tệp** | **Nội dung** |
|:---|:---|:---|
| 1 | `lab5/Lab5.ipynb` | Notebook đã chạy hết, không còn ô TODO trống |
| 2 | `lab5/outputs/lab5_ket_qua.json` | Kết quả và thời gian của cả 8 bước |
| 3 | `lab5/outputs/report_lab05.md` | Báo cáo, kèm bảng so sánh công bằng và phần bình luận về tính công bằng |
| 4 | Ảnh chụp `localhost:4040` | Tab **Jobs**, tab **Stages** (thấy rõ ranh giới shuffle), tab **SQL/DataFrame** (đồ thị kế hoạch có nút `Exchange`), tab **Storage** (sau khi cache) |
| 5 | Ảnh chụp kết quả `spark-submit` trong container | Bảng doanh thu 6 dòng và thời gian |

## 3.2. Bảng chấm điểm gợi ý

| **Tiêu chí** | **Điểm** |
|:---|---:|
| D1 — SparkSession đúng cấu hình, đọc được Spark UI | 10 |
| D2 — RDD WordCount khớp Buổi 04, giải thích được `reduceByKey` vs `groupByKey` | 15 |
| D3 — DataFrame cho kết quả khớp tuyệt đối | 15 |
| D4 — Ba truy vấn Spark SQL đúng, đọc được ý nghĩa nghiệp vụ | 15 |
| D5 — Chứng minh lazy evaluation, đếm và giải thích `Exchange` | 20 |
| D6 — Đo được lợi ích `cache()`, nêu đúng quy tắc dùng | 10 |
| D7 — Bảng so sánh **và** phân tích tính công bằng | 10 |
| D8 — Chạy Spark trong cụm, bảng so sánh công bằng | 5 |
| **Tổng** | **100** |

## 3.3. Những lỗi thường gặp và cách xử lý

| **Lỗi** | **Nguyên nhân** | **Cách xử lý** |
|:---|:---|:---|
| `JAVA_HOME is not set` / `Java gateway process exited` | Chưa cài Java hoặc sai phiên bản | Cài JDK 17; PySpark 4.x **bắt buộc** Java ≥ 17 |
| `Python worker exited unexpectedly` | Phiên bản Python của driver và worker khác nhau | Đặt `PYSPARK_PYTHON=sys.executable` |
| `Initial job has not accepted any resources` (treo mãi) | Nối vào cụm standalone mà không đủ tài nguyên, hoặc **lệch phiên bản** | Dùng `.master("local[*]")` |
| `java.io.InvalidClassException` | PySpark trên máy ≠ Spark trong container | Cài đúng `pyspark==3.5.1` hoặc dùng `local[*]` |
| `Port 4040 already in use` | Còn một SparkSession khác đang sống | Không phải lỗi — Spark tự nhảy sang 4041. Muốn dọn thì `spark.stop()` |
| Chạy ô lần hai thì rất chậm | Không cache, mỗi action đọc lại tệp từ đầu | Thêm `.cache()` + một `.count()` |
| `OutOfMemoryError` ở driver | `collect()` kéo **toàn bộ** dữ liệu về driver | Dùng `show(20)`, `take(n)`, hoặc `write` ra tệp |
| `AnalysisException: cannot resolve 'col'` | Sai tên cột, hoặc chưa `createOrReplaceTempView` | `df.printSchema()` trước khi viết truy vấn |
| Kết quả WordCount lệch với Buổi 04 | Quy tắc chuẩn hóa khác nhau | Dùng lại **đúng** `str.maketrans` của Buổi 04 |
| 200 task tí hon cho một phép `groupBy` | `spark.sql.shuffle.partitions` mặc định là 200 | Đặt bằng 2–3 lần số lõi |
| `spark.stop()` rồi vẫn dùng `spark` | Phiên đã đóng | Tạo lại bằng `SparkSession.builder...getOrCreate()` |
| Cảnh báo `NativeCodeLoader` | Thiếu thư viện native của Hadoop | **Không phải lỗi** — bỏ qua |

> **Mẹo gỡ lỗi quan trọng nhất của buổi học:** khi một job Spark chạy lâu bất thường, đừng đoán — **mở `localhost:4040`, vào tab Stages, tìm stage có số task nhiều nhất hoặc thời gian lệch nhau nhất giữa các task.** Task lệch nhau lớn nghĩa là **data skew**: một khóa chiếm phần lớn dữ liệu, đúng hiện tượng đã gặp ở bài tập 4 của Buổi 04.

## 3.4. Kết nối tới các buổi tiếp theo

| **Buổi** | **Chủ đề** | **Liên hệ với buổi này** |
|:---|:---|:---|
| **06** | Data Wrangling | Làm sạch dữ liệu bằng chính DataFrame API của hôm nay, ở quy mô phân tán |
| **07** | Feature Engineering | `withColumn` và `CASE WHEN` trở thành bước tạo đặc trưng cho mô hình |
| **08+** | Spark Streaming / Kafka | Structured Streaming dùng **y hệt** DataFrame API — chỉ khác nguồn dữ liệu là luồng thay vì tệp |
| **13** | Học máy | Spark MLlib chạy trên DataFrame; `cache()` của D6 là bắt buộc vì thuật toán học máy **lặp hàng chục vòng** |

> Hôm nay bạn thấy `cache()` chỉ giúp nhanh hơn 3 lần trên 8 vòng lặp. Buổi 13 sẽ chạy thuật toán lặp **hàng chục vòng** trên dữ liệu lớn hơn — và ở đó, quên `cache()` là chênh nhau **hàng chục lần**, không phải ba.

---

# PHỤ LỤC A — Sáu bài tập về nhà và đáp án

## Bài 1 — WordCount có lọc từ ngắn

Sửa chuỗi RDD của D2: chỉ giữ những từ **dài từ 4 ký tự trở lên**, rồi lấy 10 từ đứng đầu.

**Đáp án:** thêm một `.filter(lambda tu: len(tu) >= 4)` **ngay sau `flatMap`**. Mọi từ dưới 4 ký tự biến mất — kể cả những từ tần suất cao nhất như `một` (43), `dữ` (27), `của` (26), `là` (20) và `máy` (20). Đứng đầu còn lại là `liệu` (27), rồi `spark` (13), sau đó là ba từ cùng 10 lần: `cùng`, `khối`, `tính` (bằng tần suất thì `takeOrdered` sắp theo bảng chữ cái).

> **Điểm dạy — vì sao đặt `filter` ngay sau `flatMap` chứ không phải cuối chuỗi?** Vì `filter` là phép **narrow**: đặt nó **trước** `reduceByKey` thì lượng dữ liệu đi qua shuffle giảm hẳn. Đặt sau thì đã shuffle xong rồi mới bỏ đi. Cùng kết quả, khác hiệu năng. Đây chính là điều Catalyst tự làm cho DataFrame — nhưng với RDD thì **bạn phải tự làm**.

## Bài 2 — Top 3 sản phẩm trong mỗi ngành hàng (hàm cửa sổ)

Dùng `Window.partitionBy("category").orderBy(desc("doanh_thu"))` và `row_number()`.

**Đáp án — hai dòng đầu:** ngành **Gia dụng** dẫn đầu là *Quạt điều hòa* (26.796.034.000), ngành **Điện tử** dẫn đầu là *Bàn phím cơ* (18.430.372.000).

> **Điểm dạy:** hàm cửa sổ là thứ MapReduce **gần như không làm nổi** — nó cần xếp hạng **trong từng nhóm**, tức là reducer phải giữ toàn bộ nhóm trong bộ nhớ rồi sắp xếp. Spark làm bằng một biểu thức. Đây là ví dụ rõ nhất cho việc **API mạnh hơn thì bài toán khả thi hơn**, chứ không chỉ là nhanh hơn.

## Bài 3 — `reduceByKey` với `groupByKey`

Chạy WordCount hai lần, một lần `reduceByKey`, một lần `groupByKey().mapValues(sum)`. So thời gian và so kế hoạch trong Spark UI.

**Đáp án:** trên 7 KB dữ liệu, chênh lệch thời gian **rất nhỏ** — đôi khi `groupByKey` còn nhanh hơn do nhiễu đo.

> **Điểm dạy, và là điểm dạy trung thực nhất của phụ lục này:** kết quả "không thấy khác biệt" **không** bác bỏ lời khuyên. Hãy bắt học viên nhìn vào **`Shuffle Read`/`Shuffle Write`** trong tab Stages thay vì nhìn đồng hồ: `reduceByKey` shuffle **373 cặp**, `groupByKey` shuffle **1.144 cặp** — gấp 3 lần. Trên 1 tỷ dòng, tỷ lệ đó vẫn giữ nguyên nhưng con số tuyệt đối thành hàng trăm GB đi qua mạng.
>
> **Đo hiệu năng trên dữ liệu tí hon thì kết luận cũng tí hon.** Hãy đo *khối lượng dữ liệu qua shuffle*, đại lượng đó không bị nhiễu đo che lấp.

## Bài 4 — Khách hàng có giá trị cao nhất

Tính tổng chi tiêu theo `customer_id`, lấy 5 khách đứng đầu.

**Đáp án:**

| **customer_id** | **tổng chi tiêu** |
|:---|---:|
| C08849 | 40.774.000 |
| C03442 | 36.850.000 |
| C10794 | 33.743.000 |
| C13557 | 33.369.000 |
| C21319 | 33.137.000 |

Tổng số khách hàng khác nhau: **24.989**.

> **Điểm dạy:** đây là phép `groupBy` trên khóa có **cardinality rất cao** — 24.989 khóa thay vì 6. Hãy so số task và `Shuffle Write` của phép này với phép `groupBy("category")` ở D3. Cardinality của khóa là yếu tố quyết định chi phí shuffle, và là điều cần cân nhắc **trước khi** viết một phép gom nhóm trên dữ liệu lớn.

## Bài 5 — `join` giữa hai DataFrame

Tạo một DataFrame nhỏ ánh xạ `store_id` → `khu_vuc` (Bắc / Trung / Nam), rồi `join` với bảng giao dịch và tính doanh thu theo khu vực.

**Đáp án:** doanh thu tập trung ở khu vực **Nam** (TP HCM 3 cửa hàng + Bình Dương + Cần Thơ), chiếm hơn một nửa tổng doanh thu 206.744.802.000 VND.

> **Điểm dạy — `broadcast join`, thứ đáng giá nhất trong bài này.** Bảng danh mục chỉ có 10 dòng. Mặc định Spark sẽ shuffle **cả hai** bảng theo khóa nối — cực kỳ lãng phí. Bọc bảng nhỏ trong `F.broadcast(df_nho)` để Spark **gửi nguyên bảng nhỏ tới mọi executor** và nối cục bộ, **không shuffle bảng lớn**.
>
> Hãy so `explain()` hai cách: một bên có `Exchange` cho cả hai nhánh, một bên chỉ có `BroadcastExchange` cho nhánh nhỏ. Đây là kỹ thuật tối ưu Spark được dùng nhiều nhất trong thực tế.

## Bài 6 — Ghi kết quả ra Parquet và so với CSV

Ghi `doanh_thu` ra cả hai định dạng, so kích thước tệp và thời gian đọc lại.

**Đáp án:** Parquet nhỏ hơn và đọc nhanh hơn đáng kể, đồng thời **giữ nguyên kiểu dữ liệu** — đọc lại không cần `inferSchema`.

> **Điểm dạy:** Parquet là định dạng **theo cột**, có nén và có lược đồ nhúng bên trong. Ba hệ quả: (1) chỉ đọc những cột cần dùng — chính là `column pruning` đã thấy ở D5; (2) nén tốt hơn nhiều vì dữ liệu cùng cột thì cùng kiểu; (3) không phải đoán kiểu.
>
> **Quy tắc nghề nghiệp:** CSV để **trao đổi** với người và hệ thống ngoài; Parquet để **lưu trữ và xử lý**. Buổi 06 và Buổi 13 sẽ dùng Parquet cho toàn bộ dữ liệu trung gian.

---

# PHỤ LỤC B — Bảng tra cứu API PySpark

## B.1. Transformation (lười — chưa tính gì)

| **RDD** | **DataFrame** | **Công dụng** | **Narrow / Wide** |
|:---|:---|:---|:---|
| `.map(f)` | `.withColumn()`, `.select()` | Biến đổi từng phần tử | Narrow |
| `.flatMap(f)` | `explode(split(...))` | Một vào → nhiều ra | Narrow |
| `.filter(f)` | `.filter()`, `.where()` | Lọc | Narrow |
| `.union(other)` | `.union()` | Gộp | Narrow |
| `.reduceByKey(f)` | `.groupBy().agg()` | Gom nhóm + tổng hợp | **Wide** |
| `.groupByKey()` | — | Gom nhóm (nên tránh) | **Wide** |
| `.sortBy(f)` | `.orderBy()` | Sắp xếp | **Wide** |
| `.distinct()` | `.distinct()`, `.dropDuplicates()` | Loại trùng | **Wide** |
| `.join(other)` | `.join()` | Nối bảng | **Wide** |
| `.repartition(n)` | `.repartition(n)` | Đổi số phân vùng (có shuffle) | **Wide** |
| `.coalesce(n)` | `.coalesce(n)` | **Giảm** số phân vùng (không shuffle) | Narrow |

## B.2. Action (kích hoạt tính toán)

| **Lệnh** | **Trả về** | **Cảnh báo** |
|:---|:---|:---|
| `.count()` | Số dòng | Chạy lại **toàn bộ** lineage mỗi lần gọi |
| `.collect()` | **Toàn bộ** dữ liệu về driver | **Nguy hiểm** — tràn RAM driver |
| `.take(n)` / `.first()` | n phần tử đầu | An toàn |
| `.show(n)` | In bảng ra màn hình | Chỉ dùng cho DataFrame |
| `.takeOrdered(n, key)` | n phần tử nhỏ nhất theo `key` | Cách lấy top-N an toàn cho RDD |
| `.saveAsTextFile(đd)` / `.write...` | Ghi ra tệp | Thư mục đích phải chưa tồn tại, hoặc dùng `.mode("overwrite")` |
| `.foreach(f)` | Không | Chạy `f` trên executor, **không** ở driver |

## B.3. Cấu hình hay dùng

| **Tham số** | **Mặc định** | **Nên đặt** |
|:---|:---|:---|
| `spark.sql.shuffle.partitions` | 200 | 2–3 lần số lõi khi chạy trên máy cá nhân |
| `spark.driver.memory` | 1g | 4g khi `collect()` bảng lớn |
| `spark.executor.memory` | 1g | Tùy RAM mỗi máy trong cụm |
| `spark.sql.adaptive.enabled` | `true` (Spark 3+) | Giữ nguyên — AQE tự gộp phân vùng nhỏ sau shuffle |
| `spark.ui.showConsoleProgress` | `true` | `false` trong notebook cho đỡ rối màn hình |

## B.4. Cấu trúc thư mục của buổi học

```text
lab5/
├── Buoi_05_Huong_Dan_Thuc_Hanh.md   # tài liệu này
├── Lab5.ipynb                       # notebook thực hành
├── spark_jobs/
│   └── revenue_hdfs.py              # D8 — Spark chạy trong container, đọc HDFS
├── images/                          # ảnh chụp Spark UI dùng trong tài liệu
│   ├── 13_spark_master.png  14_spark_jobs.png  15_spark_stages.png
│   ├── 16_spark_stage_wordcount.png  17_spark_sql_list.png
│   └── 18_spark_sql_plan.png  19_spark_storage.png  20_spark_executors.png
└── outputs/
    ├── lab5_ket_qua.json
    └── report_lab05.md
```

---

# PHỤ LỤC C — Xử lý sự cố

**Thứ tự kiểm tra khi Spark không khởi động được:**

```bash
# 1. Java có chưa, phiên bản nào?
java -version

# 2. PySpark cài chưa, bản nào?
python3 -c "import pyspark; print(pyspark.__version__)"

# 3. Thử tạo phiên tối giản
python3 -c "
from pyspark.sql import SparkSession
s = SparkSession.builder.master('local[1]').getOrCreate()
print('OK', s.version); s.stop()"
```

**Khi cần chạy Spark trong cụm Docker (D8):**

```bash
# Cụm Spark có sống không?
docker compose ps spark-master spark-worker

# Container có nhìn thấy NameNode không?
docker exec bigdata-spark-master getent hosts hadoop-namenode

# Dữ liệu Buổi 04 còn trên HDFS không?
docker exec bigdata-hadoop-namenode hdfs dfs -ls -h /user/bigdata/lab4/input
```

> **Nếu HDFS trống** (ví dụ đã chạy `docker compose down -v` sau Buổi 04): chạy lại D2 của Buổi 04 để đưa hai tệp lên, hoặc chạy nhanh bằng dòng lệnh:
>
> ```bash
> docker cp data/raw/transactions.csv bigdata-hadoop-namenode:/tmp/
> docker cp data/raw/wordcount_corpus.txt bigdata-hadoop-namenode:/tmp/
> docker exec bigdata-hadoop-namenode bash -c \
>   "hdfs dfs -mkdir -p /user/bigdata/lab4/input && hdfs dfs -put -f /tmp/transactions.csv /tmp/wordcount_corpus.txt /user/bigdata/lab4/input/"
> ```

**Nếu máy học viên không chạy được Docker:** D1–D7 **vẫn chạy trọn vẹn** vì chúng chỉ cần PySpark và hai tệp trong `data/raw/`. Chỉ D8 cần cụm — bù bằng ảnh chụp màn hình của giảng viên, và học viên vẫn phải trả lời được câu hỏi *"vì sao bảng so sánh ở D7 chưa công bằng"*.

**Nếu thiếu `lab4/outputs/lab4_timings.json`:** notebook sẽ dùng bộ số mặc định đo trên máy soạn bài (~29,2 giây mỗi job) và in cảnh báo. Kết quả tính toán không bị ảnh hưởng, chỉ có bảng so sánh ở D7 là không phản ánh máy của học viên.
