from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract


def main():
    spark = SparkSession.builder \
        .appName("ProductsCategories") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    products_data = [
        (1, "Product1"),
        (2, "Product2"),
        (3, "Product3"),
        (4, "Product4"),
        (5, "Product5"),
        (6, "Product6"),
        (7, "Product7"),
        (8, "Product8"),
        (9, "Product9"),
        (10, "Product10")
    ]
    products_columns = ["product_id", "product_name"]

    categories_data = [
        (1, "Category1"),
        (2, "Category2"),
        (3, "Category3"),
        (4, "Category4"),
        (5, "Category5")
    ]
    categories_columns = ["category_id", "category_name"]

    product_categories_data = [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
        (6, 1),
        (7, 2),
        (8, 3),
        (9, None),
        (10, None)
    ]
    product_categories_columns = ["product_id", "category_id"]

    products = spark.createDataFrame(products_data, products_columns)
    categories = spark.createDataFrame(categories_data, categories_columns)
    product_categories = spark.createDataFrame(product_categories_data, product_categories_columns)

    products = products.withColumn("product_num", regexp_extract(col("product_name"), "(\d+)", 1).cast("int"))

    # Получение пар "Имя продукта – Имя категории"
    products_with_categories = products.join(product_categories, "product_id", "left")
    result = products_with_categories.join(categories, products_with_categories.category_id == categories.category_id,
                                           "left")
    pairs_df = result.select(products.product_name, categories.category_name).orderBy("product_num")

    # Продукты без категорий
    products_without_categories = products.join(product_categories, "product_id", "left") \
        .filter(col("category_id").isNull()) \
        .select("product_name") \
        .orderBy("product_num")

    print("Пары 'Имя продукта – Имя категории':")
    pairs_df.show()

    print("Продукты без категорий:")
    products_without_categories.show()

    spark.stop()


if __name__ == "__main__":
    main()

# Результат:

# Пары 'Имя продукта – Имя категории':
# +------------+-------------+
# |product_name|category_name|
# +------------+-------------+
# |    Product1|    Category1|
# |    Product2|    Category2|
# |    Product3|    Category3|
# |    Product4|    Category4|
# |    Product5|    Category5|
# |    Product6|    Category1|
# |    Product7|    Category2|
# |    Product8|    Category3|
# |    Product9|         NULL|
# |   Product10|         NULL|
# +------------+-------------+
#
# Продукты без категорий:
# +------------+
# |product_name|
# +------------+
# |    Product9|
# |   Product10|
# +------------+
