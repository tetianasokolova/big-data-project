import pyspark.sql.functions as f


def change_column_names(title_akas_df):
    title_akas_df = title_akas_df.withColumnRenamed('titleId', 'title_id').withColumnRenamed('isOriginalTitle',
                                                                                             'is_original_title')
    return title_akas_df

def change_is_original_title_column_type(title_akas_df):
    return title_akas_df.withColumn('is_original_title', title_akas_df['is_original_title'].cast('boolean'))


def change_n_to_none(title_akas_df):
    columns_with_n = ['region', 'language', 'types', 'attributes']
    for column in columns_with_n:
        title_akas_df = title_akas_df.withColumn(column,
                                                 f.when(f.col(column).isin("\\N"), None).otherwise(f.col(column)))
    return title_akas_df

def fill_null_region_values(title_akas_df):
    title_akas_df=title_akas_df.withColumn('region',f.when((f.col('region').isNull()) & (f.col('is_original_title')==True),'original_region')
                               .otherwise(f.col('region')))
    return title_akas_df

def drop_types_column(title_akas_df):
    return title_akas_df.drop(f.col('types'))

def drop_attributes_column(title_akas_df):
    return title_akas_df.drop(f.col('attributes'))

def drop_is_original_title_column(title_akas_df):
    return title_akas_df.drop(f.col('is_original_title'))

def title_akas_postprocess(title_akas_df):
    title_akas_df = change_column_names(title_akas_df)
    title_akas_df = change_is_original_title_column_type(title_akas_df)
    title_akas_df = change_n_to_none(title_akas_df)
    title_akas_df=fill_null_region_values(title_akas_df)
    title_akas_df=drop_types_column(title_akas_df)
    title_akas_df=drop_attributes_column(title_akas_df)
    title_akas_df=drop_is_original_title_column(title_akas_df)

    return title_akas_df
