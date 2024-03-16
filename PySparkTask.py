from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit

def get_product_category_pairs(products_df, categories_df):

    product_category_pairs = products_df.join(categories_df, products_df["product_id"] == categories_df["product_id"], "left") \
        .select(products_df["product_name"], categories_df["category_name"]) \
        .where(categories_df["category_name"].isNotNull())

    products_without_categories = products_df.join(categories_df, products_df["product_id"] == categories_df["product_id"], "left") \
        .select(products_df["product_name"]) \
        .where(categories_df["category_name"].isNull())

    return product_category_pairs.union(products_without_categories.withColumn("category_name", lit("None")))

spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

# Создаем тестовый датафрейм products_df
products_data = [
    Row(product_id=1, product_name='Product A'),
    Row(product_id=2, product_name='Product B'),
    Row(product_id=3, product_name='Product C')
]
products_df = spark.createDataFrame(products_data)

# Создаем тестовый датафрейм categories_df
categories_data = [
    Row(product_id=1, category_name='Category X'),
    Row(product_id=2, category_name='Category Y')
]
categories_df = spark.createDataFrame(categories_data)

result_df = get_product_category_pairs(products_df, categories_df)

spark.stop()