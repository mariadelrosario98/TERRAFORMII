# 📊 Taller 3 – Minería de Grandes Volúmenes de Datos  
### Benchmark de Análisis de Logs a Escala (AWS S3 + EC2)

Este proyecto implementa y compara diferentes enfoques de procesamiento para grandes volúmenes de datos alojados en **Amazon S3**, utilizando instancias **EC2** como entorno de ejecución.  
El objetivo fue evaluar el rendimiento, uso de memoria y CPU de distintas herramientas analíticas: **Python**, **Pandas**, **Polars**, **DuckDB** y **Apache Spark**.

---

## 🧱 1. Infraestructura y Preparación de Datos

### 📁 Estructura de Datos en S3
Los datos se almacenaron en el bucket:
s3a://terraform-51257688b24ec567/


Cada carpeta representa una carga de trabajo específica:

| Folder | Workload | Propósito |
|---------|-----------|-----------|
| `5gb/`  | 5 GB  | Línea base de rendimiento |
| `10gb/` | 10 GB | Escalabilidad inicial |
| `15gb/` | 15 GB | Rendimiento intermedio |
| `20gb/` | 20 GB | Carga a gran escala |
| `25gb/` | 25 GB | Límite superior del benchmark |

Los archivos `.json` (~94 KB cada uno) contienen arreglos de logs con campos:

- `timestamp` → permite calcular tasas de logs por segundo  
- `message` → contiene el código HTTP (`200`, `404`, `500`)  
- `log_level`, `user_id`, `session_id`, etc.

---

## ⚙️ 2. Metodología de Implementación

Se desarrollaron y compararon dos estrategias principales de procesamiento desde S3.

### 🧩 Enfoque Híbrido (Hybrid I/O)
Usado en **Pandas** y **Polars** por problemas de autenticación y versión.  
El flujo fue:

1. Descarga secuencial de archivos con `boto3`.
2. Lectura en memoria (`io.StringIO`).
3. Procesamiento vectorizado con Pandas o Polars.

| Herramienta | Estrategia | Descripción |
|--------------|-------------|-------------|
| Python / Pandas / Polars | Descarga secuencial + procesamiento vectorizado | Iteración lenta vía boto3, luego procesamiento local eficiente. |

### 🚀 Enfoque Nativo (Native S3 I/O)
Intento original de lectura paralela directa desde S3.

| Herramienta | Estrategia | Descripción |
|--------------|-------------|-------------|
| Polars (original) | Conector S3 integrado + paralelismo | Bloqueado por errores de autenticación y formato; se adoptó el enfoque híbrido. |

---

## 💻 3. Tutorial: Comandos Esenciales

### 🧩 Preparación del entorno
Cada metodología tenía su propio ambiente virtual:

```bash
mkdir -p ~/ex-polars
cd ~/ex-polars
python3.12 -m venv .venv
. .venv/bin/activate
```
##📦 Instalación de dependencias
# Polars o Pandas
pip install polars boto3
pip install pandas boto3

# Python puro
pip install boto3

# ⚡ Ejecución del Benchmark

Cada prueba se medía con `/usr/bin/time -v`:

```bash
/usr/bin/time -v python main.py s3://[NOMBRE-BUCKET]/[TAMAÑO-GB]


**Ejemplo (Polars, 10 GB):**

`/usr/bin/time -v python main.py s3://terraform-51257688b24ec567/10gb`
```
* * * * *

🔥 4. Configuración de Spark y Conexión a S3
--------------------------------------------

### 4.1 Solución de Incompatibilidad Java / PySpark
```
`pip install pyspark==3.2.4
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`
```
### 4.2 Configuración de Conectividad a S3

| Parámetro | Propósito |
| --- | --- |
| `--packages org.apache.hadoop:hadoop-aws:3.3.1` | Librerías de AWS y Hadoop para S3 |
| `--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"` | Activa el sistema de archivos S3A |

### 4.3 Permisos de Acceso a Módulos (Java 17)

`--conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\
--add-opens=java.base/java.nio=ALL-UNNAMED\
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"`

### 4.4 Ejemplo Final (20 GB)
```
`/usr/bin/time -v env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 spark-submit\
  --master local[*]\
  --packages org.apache.hadoop:hadoop-aws:3.3.1\
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"\
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/sun.nio.ch=ALL-UNNAMED\
  --add-opens=java.base/java.nio=ALL-UNNAMED\
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"\
  /home/ssm-user/ex-spark/main.py s3a://terraform-51257688b24ec567/20gb`
```
* * * * *

🧠 5. Organización de la Infraestructura
----------------------------------------

Estructura modular por carpetas:

`~/ex-polars/
    ├── main.py
    ├── .venv/
~/ex-pandas/
~/ex-python/
~/ex-spark/`

Cada carpeta tiene su propio entorno virtual (`.venv`) para evitar conflictos de dependencias entre frameworks.

* * * * *

📈 6. Resultados del Benchmark
------------------------------

| Workload (GB) | Tool | Time (min) | Peak RAM (MB) | Avg CPU (%) |
| --- | --- | --- | --- | --- |
| 5 | Pandas | 6.98 | 2,192 | 12 |
| 5 | Polars | 6.59 | 804 | 7 |
| 5 | Python | 6.21 | 323 | 7 |
| 5 | Spark | 1.60 | 130 | 119 |
| 5 | DuckDB | 1.14 | 126 | 60 |
| 10 | Pandas | 14.42 | 4,297 | 11 |
| 10 | Polars | 12.05 | 563 | 8 |
| 10 | Spark | 2.68 | 1,252 | 155 |
| 10 | DuckDB | 2.35 | 143 | 60 |

* * * * *

🧩 7. Conclusiones del Benchmark
--------------------------------

### ⚡ Rendimiento Bruto

-   **DuckDB** y **Spark** fueron las herramientas más rápidas.

-   Spark mostró una **escalabilidad casi lineal**: duplicar el tamaño de los datos duplicó el tiempo de ejecución.

### 🦆 DuckDB: Procesamiento Vectorizado

-   Motor columnar **OLAP** con lectura directa y eficiente.

-   Mínimo uso de **RAM (~150 MB)**.

-   **CPU estable (~60%)** y tiempo bajo.

### 🔥 Spark: Paralelismo Distribuido

-   Lectura paralela desde S3 y **DAG optimizado**.

-   **CPU entre 119--155%**, aprovechando múltiples núcleos.

-   Mayor uso de RAM por la **sobrecarga de la JVM**.

### 💾 Sobrecarga JVM e In-Memory Processing

-   La JVM introduce sobrecarga por gestión de objetos y *garbage collection*.

-   Spark contrarresta esto con *in-memory processing*.

-   El framework **Tungsten** reduce el uso del heap y mejora el rendimiento.

* * * * *

🧪 8. Análisis de la Data
-------------------------

La data sintética generada simula logs de API con distribución uniforme:

| Código HTTP | Proporción | Interpretación |
| --- | --- | --- |
| 2xx | ≈ 40% | Éxito |
| 4xx | ≈ 50% | Errores de cliente |
| 5xx | ≈ 10% | Errores de servidor (críticos) |

* * * * *

📚 9. Métricas de Evaluación
----------------------------

Las métricas fueron medidas con `/usr/bin/time -v`:

-   **Tiempo total (min)**

-   **Memoria pico (MB)**

-   **CPU promedio (%)**

* * * * *

🧾 Créditos
-----------

**Autora:**\
María del Rosario Castro Mantilla\
**Universidad EAFIT -- Maestría en Ciencia de Datos y Analítica**\
Curso: *Minería de Grandes Volúmenes de Datos -- Taller 3*
