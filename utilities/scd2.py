from pyspark.sql.functions import col, expr # *

def scd2_params():
    return dict(
        keys=["id"],
        sequence_by=col("operation_date"),
        ignore_null_updates=False,
        apply_as_deletes=expr("operation = 'DELETE'"),
        except_column_list=["operation", "operation_date", "_rescued_data"],
        stored_as_scd_type="2",
    )