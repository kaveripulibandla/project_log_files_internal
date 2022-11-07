import env


class SnowflakeHelper:
    def save_df_to_snowflake(self, df, table):
        sfOptions = {
            "sfURL": env.sfURL,
            "sfAccount": env.sfAccount,
            "sfUser": env.sfUser,
            "sfPassword": env.sfPassword,
            "sfDatabase": env.sfDatabase,
            "sfSchema": env.sfSchema,
            "sfWarehouse": env.sfWarehouse,
            "sfRole": env.sfRole
        }

        df.write.format("snowflake").options(**sfOptions).option("dbtable", "{}".format(table)).mode(
            "overwrite").options(header=True).save()


# obj = SnowflakeHelper()
# SnowflakeHelper().save_df_to_snowflake(obj, env.sf_raw_table)