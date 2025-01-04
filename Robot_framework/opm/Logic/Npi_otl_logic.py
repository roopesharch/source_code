from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import DeltaTable
import pandas as pd
from datetime import datetime, timedelta


def process_source_data(quick_base_otl_df):
    # Initialize a Spark session
    spark = SparkSession.builder.getOrCreate()
    # quick_base_otl_df = spark.createDataFrame(kelvin_npi_otl_df)
    
    # DBTITLE 1,quickbase Pyspark column transformation
    otl_quickbase_extract = quick_base_otl_df.select(
        col("Material Number").alias("material_number"),
        col("Material Description").alias("material_description"),
        col("International Brand").alias("international_brand"),
        col("Country").alias("country"),
        to_date(col("Product Availability Due Date")).alias(
            "product_availability_due_date"
        ),
        to_date(col("Product Availability Actual Date")).alias(
            "product_availability_actual_date"
        ),
        to_date(col("Available for Sales Due Date")).alias("available_for_sales_due_date"),
        to_date(col("Available for Sales Actual Date")).alias(
            "available_for_sales_actual_date"
        ),
        col("Launched On-Time?").alias("launched_on_time_"),
        col("Delivered to DC On-Time?").alias("delivered_to_dc_on_time_"),
        col("Brand Name (per Market)").alias("brand_name__per_market_"),
        col("Country Code").alias("country_code"),
        col("Region").alias("region"),
        col("SCDL Text").alias("scdl_text"),
        col("# of SKUs").alias("no_of_skus"),
        col("Rapid Launch").alias("rapid_launch"),
        col("G15 Country").alias("g15_country"),
        col("Launch Group").alias("launch_group"),
        col("Material - Therapeutic Area").alias("material___therapeutic_area"),
        col("Material - Source System").alias("material___source_system"),
        col("Launch Site").alias("launch_site"),
        to_date(col("Original Committed Launch Date")).alias(
            "original_committed_launch_date"
        ),
        col("On Time Deliver Delay Reason").alias("on_time_deliver_delay_reason"),
        col("Launch Delay Reason").alias("launch_delay_reason"),
        col("Comments").alias("comments"),
        col("Ahead of Schedule Launch").alias("ahead_of_schedule_launch"),
        col("External Customer?").alias("external_customer_"),
        to_date(col("Health Authority Approval Actual Date")).alias(
            "health_authority_approval_actual_date"
        ),
        col("Post-Approval Affiliate Delay Reason").alias(
            "post_approval_affiliate_delay_reason"
        ),
        col("Days from Approval to Product Available at DC").alias(
            "days_from_approval_to_product_available_at_dc"
        ),
        col("Days from Approval to Launch").alias("days_from_approval_to_launch"),
        col("Country Tip Tool Updated?").alias("country_tip_tool_updated_"),
        to_date(col("Date Modified")).alias("date_modified"),
        col("Last Modified By").alias("last_modified_by"),
        col("Controlled Substances").alias("controlled_substances"),
        col("Finished Good Storage Requirement").alias("finished_good_storage_requirement"),
        col("Other Special Handling Needs").alias("other_special_handling_needs"),
        col("Accelerated HA Approval").alias("accelerated_ha_approval"),
        col("Secondary DC").alias("secondary_dc"),
        col("EUROPE2 Material Code").alias("europe2_material_code"),
        col("SKU - Atlas Number").alias("sku___atlas_number"),
        col("SKU - Sustain Number").alias("sku___sustain_number"),
        col("PANDA Material Code").alias("panda_material_code"),
        col("Primary DC").alias("primary_dc"),
        col("Available for Sales Actual Date by Quarter").alias(
            "available_for_sales_actual_date_by_quarter"
        ),
        col("Available for Sales Due Date by Quarter").alias(
            "available_for_sales_due_date_by_quarter"
        ),
        col("Global Material Code").alias("global_material_code"),
        col("Sample"),
        col("Launch Template").alias("launch_template"),
        col("Launch Canceled / on Hold").alias("launch_canceled_or_on_hold"),
    ).where(
        ~(
            (upper(trim(col("Launch Canceled / on Hold"))) == "TRUE")
            | (upper(trim(col("Launch Plan Cancelled"))) == "TRUE")
        )
    )
    
    otl_quickbase_extract = otl_quickbase_extract.select(
        *otl_quickbase_extract.columns,  # Keep all previously selected columns
        to_date(col("product_availability_due_date")).alias("product_availability_plan_date"),
        to_date(col("product_availability_actual_date")).alias("product_availability_actual_date"),
        to_date(col("available_for_sales_due_date")).alias("product_first_sales_plan_date"),
        to_date(col("available_for_sales_actual_date")).alias("product_first_sales_actual_date"),
        concat(
            year(to_date(col("available_for_sales_due_date"))),
            date_format(to_date(col("available_for_sales_due_date")), 'MM')
        ).alias("product_first_sales_plan_date_join_column"),
        concat(
            year(to_date(col("product_availability_due_date"))),
            date_format(to_date(col("product_availability_due_date")), 'MM')
        ).alias("product_availability_plan_date_join_column"),
        col("launched_on_time_").alias("on_time_launch"),
        col("delivered_to_dc_on_time_").alias("on_time_launch_dc"),
        col("brand_name__per_market_").alias("brand_name"),
        when(col("material___source_system") == "[Pharm Europe2]", "EUROPE2")
        .when(col("material___source_system") == "[Pharm Panda]", "PANDA")
        .when(col("material___source_system") == "[Pharm Vacaville]", "VACAVILLE")
        .when(col("material___source_system") == "[Pharm Atlas]", "ATLAS")
        .when(col("material___source_system") == "[Pharm Sustain]", "SUSTAIN")
        .otherwise(col("material___source_system")).alias("source_system"),
        upper(trim(col("country_code"))).alias("country_code"),
        col("scdl_text").alias("resp_scdl_name"),
        coalesce(col("no_of_skus"), lit(0)).alias("sku_number_count"),
        expr("""trim(leading '0' from upper(regexp_replace(COALESCE(material_number,'-1'), '[^A-Za-z0-9]+', '')))""").alias("sku"),
        col("rapid_launch"),
        col("g15_country").alias("country_g15_code"),
        col("launch_group"),
        col("launch_site"),
        col("original_committed_launch_date"),
        col("on_time_deliver_delay_reason"),
        col("launch_delay_reason"),
        col("comments"),
        col("ahead_of_schedule_launch"),
        col("external_customer_").alias("external_customer"),
        col("health_authority_approval_actual_date"),
        col("post_approval_affiliate_delay_reason"),
        col("days_from_approval_to_product_available_at_dc"),
        col("days_from_approval_to_launch"),
        col("country_tip_tool_updated_").alias("country_tip_tool_updated"),
        col("date_modified"),
        col("last_modified_by"),
        col("controlled_substances"),
        col("finished_good_storage_requirement"),
        col("other_special_handling_needs"),
        col("accelerated_ha_approval"),
        col("secondary_dc"),
        col("europe2_material_code"),
        col("sku___atlas_number").alias("sku_atlas_number"),
        col("sku___sustain_number").alias("sku_sustain_number"),
        col("panda_material_code"),
        col("primary_dc"),
        col("available_for_sales_actual_date_by_quarter"),
        col("available_for_sales_due_date_by_quarter"),
        col("global_material_code").alias("enterprise_material_number"),
        col("Sample"),
        col("launch_template"),
        col("launch_canceled_or_on_hold")
    )

    kelvin_npi_otl_automation_df = otl_quickbase_extract.filter(
        (
            date_format(col("product_availability_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
        & (col("original_record") == 1)
    )  
    
    ###### OTL DC WW
    kelvin_npi_otl_g15_automation_df = otl_quickbase_extract.filter(
        (
            date_format(col("product_first_sales_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
        & (col("original_record") == 1)
    )
    
    ###### OTL DC G15
    # COMMAND ----------

    # DBTITLE 1,Created the dataframes with otldc_ww and otldc_g15 monthly logic
    ### otldc_ww
    current_month_period_npi_otldc_df = (
        kelvin_npi_otl_automation_df.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyyMM"),
        )
        .groupBy(col("product_availability_plan_date"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            lit("PCMD-GROWTH-OTLDC-WW").alias("metric_l1_npi"),
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_availability_plan_date").alias("fiscal_year_month_otl_dc"),
        )
    )
    ###### otl_dc_g15
    current_month_period_npi_otlg15_df = (
        kelvin_npi_otl_g15_automation_df.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyyMM"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(col("product_first_sales_plan_date"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            lit("PCMD-GROWTH-OTLG15-WW").alias("metric_l1_npi"),
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_first_sales_plan_date").alias("fiscal_year_month_otl_dc"),
        )
    )


    # COMMAND ----------

    # DBTITLE 1,Union of all current_month_period_npi_otldc_df  and current_month_period_npi_otlg15_df dataframes
    current_month_period_npi_otl_union = current_month_period_npi_otldc_df.unionByName(
        current_month_period_npi_otlg15_df.select(
            "metric_l1_npi", "on_time_launch", "total_launches", "fiscal_year_month_otl_dc"
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Created the dataframe with required columns
    current_month_period_npi_otl_df = (
        current_month_period_npi_otl_union.groupBy(
            col("fiscal_year_month_otl_dc"), col("metric_l1_npi")
        )
        .agg(
            (sum(col("on_time_launch")) / sum(col("total_launches")) * 100)
            .cast("decimal(13,1)")
            .alias("current_month_period_npi")
        )
        .select(
            current_date().alias("snapshot_date"),
            col("current_month_period_npi"),
            col("metric_l1_npi"),
            col("fiscal_year_month_otl_dc"),
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Created the dataframes for previous period columns
    previous_month_period_npi_otl_df = current_month_period_npi_otl_df.select(
        col("current_month_period_npi").alias("previous_month_period_npi"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("fiscal_year_month_otl_dc"), 1, 4),
                        lit("-"),
                        substring(col("fiscal_year_month_otl_dc"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    1,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("metric_l1_npi").alias("metric_l1"),
    )

    previous2_df = current_month_period_npi_otl_df.select(
        col("current_month_period_npi").alias("npi_period_2"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("fiscal_year_month_otl_dc"), 1, 4),
                        lit("-"),
                        substring(col("fiscal_year_month_otl_dc"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    2,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("metric_l1_npi").alias("metric_l1"),
    )

    previous3_df = current_month_period_npi_otl_df.select(
        col("current_month_period_npi").alias("npi_period_3"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("fiscal_year_month_otl_dc"), 1, 4),
                        lit("-"),
                        substring(col("fiscal_year_month_otl_dc"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    3,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("metric_l1_npi").alias("metric_l1"),
    )

    previous4_df = current_month_period_npi_otl_df.select(
        col("current_month_period_npi").alias("npi_period_4"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("fiscal_year_month_otl_dc"), 1, 4),
                        lit("-"),
                        substring(col("fiscal_year_month_otl_dc"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    4,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("metric_l1_npi").alias("metric_l1"),
    )

    previous5_df = current_month_period_npi_otl_df.select(
        col("current_month_period_npi").alias("npi_period_5"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("fiscal_year_month_otl_dc"), 1, 4),
                        lit("-"),
                        substring(col("fiscal_year_month_otl_dc"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    5,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("metric_l1_npi").alias("metric_l1"),
    )


    # COMMAND ----------

    # DBTITLE 1,By joining & created the final dataframe with required columns
    npi_month_wise_df = (
        current_month_period_npi_otl_df.alias("current_month_period_npi")
        .join(
            previous_month_period_npi_otl_df.alias("previous_month_period_npi_otl"),
            (
                col("current_month_period_npi.metric_l1_npi")
                == col("previous_month_period_npi_otl.metric_l1")
            )
            & (
                col("current_month_period_npi.fiscal_year_month_otl_dc")
                == col("previous_month_period_npi_otl.year_month_number")
            ),
            "left",
        )
        .join(
            previous2_df.alias("previous2"),
            (col("current_month_period_npi.metric_l1_npi") == col("previous2.metric_l1"))
            & (
                col("current_month_period_npi.fiscal_year_month_otl_dc")
                == col("previous2.year_month_number")
            ),
            "left",
        )
        .join(
            previous3_df.alias("previous3"),
            (col("current_month_period_npi.metric_l1_npi") == col("previous3.metric_l1"))
            & (
                col("current_month_period_npi.fiscal_year_month_otl_dc")
                == col("previous3.year_month_number")
            ),
            "left",
        )
        .join(
            previous4_df.alias("previous4"),
            (col("current_month_period_npi.metric_l1_npi") == col("previous4.metric_l1"))
            & (
                col("current_month_period_npi.fiscal_year_month_otl_dc")
                == col("previous4.year_month_number")
            ),
            "left",
        )
        .join(
            previous5_df.alias("previous5"),
            (col("current_month_period_npi.metric_l1_npi") == col("previous5.metric_l1"))
            & (
                col("current_month_period_npi.fiscal_year_month_otl_dc")
                == col("previous5.year_month_number")
            ),
            "left",
        )
        .select(
            col("current_month_period_npi.*"),
            coalesce(
                col("previous_month_period_npi_otl.previous_month_period_npi"), lit("0")
            ).alias("previous_month_period_npi"),
            coalesce(col("previous2.npi_period_2"), lit("0")).alias("previous2"),
            coalesce(col("previous3.npi_period_3"), lit("0")).alias("previous3"),
            coalesce(col("previous4.npi_period_4"), lit("0")).alias("previous4"),
            coalesce(col("previous5.npi_period_5"), lit("0")).alias("previous5"),
        )
    )

    # COMMAND ----------

    # DBTITLE 1,inserting the Previous & current month NPI_OTL Table on the basis of Close_calendar +11
    npi_otl_monthly_aggregation_df = DeltaTable.forName(
        spark, f"{database_name}.{SystemConfig.tables_list['npi_otl_monthly_aggregation']}"
    )
    fiscal_calendar_month_start_date_df = (
        Read()
        .table("l2", "jnj_calendar")
        .filter(col("calendar_date") == current_date())
        .select(
            when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
            .otherwise(0)
            .alias("run_flag")
        )
        .first()["run_flag"]
    )
    if fiscal_calendar_month_start_date_df == 1:
        npi_otl_monthly_aggregation_df.delete(
            "fiscal_year_month_otl_dc = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
        )
        npi_otl_monthly_aggregation_insert_df = npi_month_wise_df.filter(
            (
                col("fiscal_year_month_otl_dc")
                == date_format(
                    to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
                )
            )
        )
        Utils.insertintoTable(
            npi_otl_monthly_aggregation_insert_df,
            env,
            SystemConfig.tables_list["npi_otl_monthly_aggregation"],
        )
    else:
        print("Stop")

    # COMMAND ----------

    # DBTITLE 1,Created the dataframes with otldc_ww and otldc_g15 yearly logic
    ###### otldc_ww
    actual_ytd_period_npi_otldc_df = (
        kelvin_npi_otl_automation_df.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyy"),
        )
        .filter(
            year(col("product_availability_plan_date"))
            == year(add_months(current_date(), -1))
        )
        .groupBy(col("product_availability_plan_date"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            lit("PCMD-GROWTH-OTLDC-WW").alias("metric_l1_npi_year"),
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_availability_plan_date").alias("fiscal_year_otl"),
        )
    )
    ###### otldc_g15
    actual_ytd_period_npi_otlg15_df = (
        kelvin_npi_otl_g15_automation_df.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyy"),
        )
        .filter(
            (col("country_g15_code") == lit("Yes"))
            & (
                year(col("product_first_sales_plan_date"))
                == year(add_months(current_date(), -1))
            )
        )
        .groupBy(col("product_first_sales_plan_date"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            lit("PCMD-GROWTH-OTLG15-WW").alias("metric_l1_npi_year"),
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_first_sales_plan_date").alias("fiscal_year_otl"),
        )
    )


    # COMMAND ----------

    # DBTITLE 1,Union of all actual_ytd_period_npi_otldc_df  and actual_ytd_period_npi_otlg15_df dataframes
    actual_ytd_period_npi_otl_union = actual_ytd_period_npi_otldc_df.unionByName(
        actual_ytd_period_npi_otlg15_df.select(
            "metric_l1_npi_year", "on_time_launch", "total_launches", "fiscal_year_otl"
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Created final dataframe with required columns
    actual_ytd_period_npi_otl_df = (
        actual_ytd_period_npi_otl_union.groupBy(
            col("fiscal_year_otl"), col("metric_l1_npi_year")
        )
        .agg(
            (sum(col("on_time_launch")) / sum(col("total_launches")) * 100)
            .cast("decimal(13,1)")
            .alias("actual_ytd_period_npi")
        )
        .select(
            current_date().alias("snapshot_date"),
            col("actual_ytd_period_npi"),
            col("metric_l1_npi_year"),
            col("fiscal_year_otl"),
            date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            ).alias("month_name_npi"),
        )
    )

    # COMMAND ----------

    # DBTITLE 1,inserting the Yearly NPI_OTL Table on the basis of Close_calendar +11
    npi_otl_yearly_aggregation_df = DeltaTable.forName(
        spark, f"{database_name}.{SystemConfig.tables_list['npi_otl_yearly_aggregation']}"
    )
    fiscal_calendar_month_start_date_df = (
        Read()
        .table("l2", "jnj_calendar")
        .filter(col("calendar_date") == current_date())
        .select(
            when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
            .otherwise(0)
            .alias("run_flag")
        )
        .first()["run_flag"]
    )
    if fiscal_calendar_month_start_date_df == 1:
        npi_otl_yearly_aggregation_df.delete(
            "month_name_npi = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
        )
        npi_otl_yearly_aggregation_insert_df = actual_ytd_period_npi_otl_df.filter(
            (
                col("month_name_npi")
                == date_format(
                    to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
                )
            )
            & col("fiscal_year_otl").isNotNull()
        )
        Utils.insertintoTable(
            npi_otl_yearly_aggregation_insert_df,
            env,
            SystemConfig.tables_list["npi_otl_yearly_aggregation"],
        )
    else:
        print("Stop")

    # COMMAND ----------

    # MAGIC %md
    # MAGIC                                                           BRAND AUTOMATION

    # COMMAND ----------

    # DBTITLE 1,Creating brand dataframe from live table with filters reported brand
    kelvin_brand_npi_otl_df = spark.table(
        f"{database_name}.{SystemConfig.tables_list['npi_otl']}"
    )
    kelvin_brand_npi_otl_automation_df_ww = kelvin_brand_npi_otl_df.filter(
        (
            date_format(col("product_availability_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
    )  ###### OTL DC WW
    kelvin_brand_npi_otl_g15_automation_df = kelvin_brand_npi_otl_df.filter(
        (
            date_format(col("product_first_sales_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
    )
    ###### OTL DC G15

    # COMMAND ----------

    # DBTITLE 1,Filtering OTL DC WW on the basis of aggregration column is null & not null
    ###### OTL DC WW
    kelvin_npi_automation_reported_brand_is_null_ww = (
        kelvin_brand_npi_otl_automation_df_ww.filter(
            (col("aggregation").isNull()) | (col("aggregation") == lit(""))
        )
    )
    kelvin_npi_automation_aggregation_brand_isnot_null_ww = (
        kelvin_brand_npi_otl_automation_df_ww.filter(
            (col("aggregation").isNotNull())
            & (col("aggregation") != lit(""))
            & (col("original_record") == 1)
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Filtering OTL DC G15 on the basis of aggregration column is null & not null
    ###### OTL DC G15
    kelvin_npi_automation_reported_brand_g15_isnull = (
        kelvin_brand_npi_otl_g15_automation_df.filter(
            (col("aggregation").isNull()) | (col("aggregation") == lit(""))
        )
    )
    kelvin_npi_automation_aggregation_brand_g15_notnull = (
        kelvin_brand_npi_otl_g15_automation_df.filter(
            (col("aggregation").isNotNull())
            & (col("aggregation") != lit(""))
            & (col("original_record") == 1)
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Brand when Aggregration is null
    ### otldc_ww
    current_month_period_npi_otldc_df_brand = (
        kelvin_npi_automation_reported_brand_is_null_ww.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyyMM"),
        )
        .groupBy(
            col("product_availability_plan_date"),
            col("Kelvin_reported_brand"),
            col("aggregation"),
        )
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_availability_plan_date").alias(
                "kelvin_brand_npi_fiscal_year_month"
            ),
            when(col("aggregation").isNull() | (col("aggregation") == lit("")), lit("null"))
            .otherwise(col("aggregation"))
            .alias("kelvin_brand_npi_aggregation_monthly"),
            concat(
                upper(trim(col("Kelvin_reported_brand"))),
                lit("-SERVICE-ON-TIME LAUNCH-OTL (DC)"),
            ).alias("kelvin_brand_npi_monthly_metric_uid"),
            upper(trim(col("Kelvin_reported_brand"))).alias("Kelvin_reported_brand_npi"),
        )
        .distinct()
    )


    ###### otl_dc_g15
    current_month_period_npi_otlg15_df_brand = (
        kelvin_npi_automation_reported_brand_g15_isnull.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyyMM"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(
            col("product_first_sales_plan_date"),
            col("Kelvin_reported_brand"),
            col("aggregation"),
        )
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_first_sales_plan_date").alias(
                "kelvin_brand_npi_fiscal_year_month"
            ),
            when(col("aggregation").isNull() | (col("aggregation") == lit("")), lit("null"))
            .otherwise(col("aggregation"))
            .alias("kelvin_brand_npi_aggregation_monthly"),
            concat(
                upper(trim(col("Kelvin_reported_brand"))),
                lit("-SERVICE-ON-TIME LAUNCH-OTL (G15)"),
            ).alias("kelvin_brand_npi_monthly_metric_uid"),
            upper(trim(col("Kelvin_reported_brand"))).alias("Kelvin_reported_brand_npi"),
        )
        .distinct()
    )


    # COMMAND ----------

    # DBTITLE 1,When aggregration is not null OTLDC WW
    ### otldc_ww
    current_month_period_npi_otldc_df_brand_aggregration_not_null = (
        kelvin_npi_automation_aggregation_brand_isnot_null_ww.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyyMM"),
        )
        .groupBy(col("product_availability_plan_date"), col("aggregation"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_availability_plan_date").alias(
                "kelvin_brand_npi_fiscal_year_month"
            ),
            col("aggregation").alias("kelvin_brand_npi_aggregation_monthly"),
            lit("-SERVICE-ON-TIME LAUNCH-OTL (DC)").alias(
                "kelvin_brand_npi_monthly_metric_uid"
            ),
        )
    )

    join_kelvin_current_npi_otldc_automation_aggregation_monthly_df = (
        current_month_period_npi_otldc_df_brand_aggregration_not_null.alias(
            "current_month_period_npi_otldc_df_brand_aggregration_not_null"
        )
        .join(
            kelvin_brand_npi_otl_automation_df_ww.alias(
                "kelvin_brand_npi_otl_automation_df_ww"
            ),
            (
                col("kelvin_brand_npi_otl_automation_df_ww.aggregation")
                == col(
                    "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_aggregation_monthly"
                )
            )
            & (
                date_format(
                    col(
                        "kelvin_brand_npi_otl_automation_df_ww.product_availability_plan_date"
                    ),
                    "yyyyMM",
                )
                == (
                    col(
                        "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_fiscal_year_month"
                    )
                )
            ),
            "left",
        )
        .select(
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_on_time_launch"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_total_launches"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_fiscal_year_month"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_aggregation_monthly"
            ),
            concat(
                col("kelvin_brand_npi_otl_automation_df_ww.Kelvin_reported_brand"),
                col(
                    "current_month_period_npi_otldc_df_brand_aggregration_not_null.kelvin_brand_npi_monthly_metric_uid"
                ),
            ).alias("kelvin_brand_npi_monthly_metric_uid"),
            trim(col("kelvin_brand_npi_otl_automation_df_ww.Kelvin_reported_brand")).alias(
                "Kelvin_reported_brand_npi"
            ),
        )
        .distinct()
    )

    # COMMAND ----------

    # DBTITLE 1,When aggregration is not null OTLDC G15
    ###### otl_dc_g15
    current_month_period_npi_otlg15_df_brand_aggregration_not_null = (
        kelvin_npi_automation_aggregation_brand_g15_notnull.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyyMM"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(col("product_first_sales_plan_date"), col("aggregation"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_first_sales_plan_date").alias(
                "kelvin_brand_npi_fiscal_year_month"
            ),
            col("aggregation").alias("kelvin_brand_npi_aggregation_monthly"),
            lit("-SERVICE-ON-TIME LAUNCH-OTL (G15)").alias(
                "kelvin_brand_npi_monthly_metric_uid"
            ),
        )
    )

    join_kelvin_current_npi_otlg15_automation_aggregation_monthly_df = (
        current_month_period_npi_otlg15_df_brand_aggregration_not_null.alias(
            "current_month_period_npi_otlg15_df_brand_aggregration_not_null"
        )
        .join(
            kelvin_brand_npi_otl_g15_automation_df.alias(
                "kelvin_brand_npi_otl_g15_automation_df"
            ),
            (
                col("kelvin_brand_npi_otl_g15_automation_df.aggregation")
                == col(
                    "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_aggregation_monthly"
                )
            )
            & (
                date_format(
                    col(
                        "kelvin_brand_npi_otl_g15_automation_df.product_first_sales_plan_date"
                    ),
                    "yyyyMM",
                )
                == (
                    col(
                        "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_fiscal_year_month"
                    )
                )
            ),
            "left",
        )
        .select(
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_on_time_launch"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_total_launches"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_fiscal_year_month"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_aggregation_monthly"
            ),
            concat(
                col("kelvin_brand_npi_otl_g15_automation_df.Kelvin_reported_brand"),
                col(
                    "current_month_period_npi_otlg15_df_brand_aggregration_not_null.kelvin_brand_npi_monthly_metric_uid"
                ),
            ).alias("kelvin_brand_npi_monthly_metric_uid"),
            trim(col("kelvin_brand_npi_otl_g15_automation_df.Kelvin_reported_brand")).alias(
                "Kelvin_reported_brand_npi"
            ),
        )
        .distinct()
    )


    # COMMAND ----------

    # DBTITLE 1,Brand union all data frame
    current_month_period_npi_otl_union_brand = (
        current_month_period_npi_otldc_df_brand.union(
            current_month_period_npi_otlg15_df_brand
        )
        .union(join_kelvin_current_npi_otlg15_automation_aggregation_monthly_df)
        .union(join_kelvin_current_npi_otldc_automation_aggregation_monthly_df)
    )

    # COMMAND ----------

    # DBTITLE 1,Created the dataframe with required columns
    current_month_period_npi_otl_df_brand = (
        current_month_period_npi_otl_union_brand.groupBy(
            col("kelvin_brand_npi_fiscal_year_month"),
            col("kelvin_brand_npi_monthly_metric_uid"),
            col("kelvin_brand_npi_aggregation_monthly"),
            col("Kelvin_reported_brand_npi"),
        )
        .agg(
            (
                sum(col("kelvin_brand_npi_on_time_launch"))
                / sum(col("kelvin_brand_npi_total_launches"))
                * 100
            )
            .cast("decimal(13,1)")
            .alias("current_month_period_npi_brand")
        )
        .select(
            current_date().alias("snapshot_date"),
            col("current_month_period_npi_brand"),
            col("kelvin_brand_npi_aggregation_monthly"),
            col("kelvin_brand_npi_monthly_metric_uid"),
            col("kelvin_brand_npi_fiscal_year_month"),
            col("Kelvin_reported_brand_npi"),
        )
    )

    # COMMAND ----------

    # DBTITLE 1,Created the dataframes for previous period columns
    previous_month_period_npi_otl_df_brand = current_month_period_npi_otl_df_brand.select(
        col("current_month_period_npi_brand").alias("previous_month_period_npi_brand"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 1, 4),
                        lit("-"),
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    1,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("kelvin_brand_npi_monthly_metric_uid").alias("metric_l1"),
    )

    previous2_df_brand = current_month_period_npi_otl_df_brand.select(
        col("current_month_period_npi_brand").alias("previous_month_period_npi_brand_2"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 1, 4),
                        lit("-"),
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    2,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("kelvin_brand_npi_monthly_metric_uid").alias("metric_l1"),
    )

    previous3_df_brand = current_month_period_npi_otl_df_brand.select(
        col("current_month_period_npi_brand").alias("previous_month_period_npi_brand_3"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 1, 4),
                        lit("-"),
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    3,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("kelvin_brand_npi_monthly_metric_uid").alias("metric_l1"),
    )

    previous4_df_brand = current_month_period_npi_otl_df_brand.select(
        col("current_month_period_npi_brand").alias("previous_month_period_npi_brand_4"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 1, 4),
                        lit("-"),
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    4,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("kelvin_brand_npi_monthly_metric_uid").alias("metric_l1"),
    )

    previous5_df_brand = current_month_period_npi_otl_df_brand.select(
        col("current_month_period_npi_brand").alias("previous_month_period_npi_brand_5"),
        date_format(
            to_timestamp(
                add_months(
                    concat(
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 1, 4),
                        lit("-"),
                        substring(col("kelvin_brand_npi_fiscal_year_month"), 5, 2),
                        lit("-"),
                        lit("01"),
                    ),
                    5,
                ),
                "yyyy-MM-dd",
            ),
            "yyyyMM",
        ).alias("year_month_number"),
        col("kelvin_brand_npi_monthly_metric_uid").alias("metric_l1"),
    )


    # COMMAND ----------

    # DBTITLE 1,By joining & created the final dataframe with required columns
    npi_month_wise_df_brand = (
        current_month_period_npi_otl_df_brand.alias("current_month_period_npi_otl_df_brand")
        .join(
            previous_month_period_npi_otl_df_brand.alias(
                "previous_month_period_npi_otl_df_brand"
            ),
            (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_monthly_metric_uid"
                )
                == col("previous_month_period_npi_otl_df_brand.metric_l1")
            )
            & (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_fiscal_year_month"
                )
                == col("previous_month_period_npi_otl_df_brand.year_month_number")
            ),
            "left",
        )
        .join(
            previous2_df_brand.alias("previous2"),
            (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_monthly_metric_uid"
                )
                == col("previous2.metric_l1")
            )
            & (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_fiscal_year_month"
                )
                == col("previous2.year_month_number")
            ),
            "left",
        )
        .join(
            previous3_df_brand.alias("previous3"),
            (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_monthly_metric_uid"
                )
                == col("previous3.metric_l1")
            )
            & (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_fiscal_year_month"
                )
                == col("previous3.year_month_number")
            ),
            "left",
        )
        .join(
            previous4_df_brand.alias("previous4"),
            (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_monthly_metric_uid"
                )
                == col("previous4.metric_l1")
            )
            & (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_fiscal_year_month"
                )
                == col("previous4.year_month_number")
            ),
            "left",
        )
        .join(
            previous5_df_brand.alias("previous5"),
            (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_monthly_metric_uid"
                )
                == col("previous5.metric_l1")
            )
            & (
                col(
                    "current_month_period_npi_otl_df_brand.kelvin_brand_npi_fiscal_year_month"
                )
                == col("previous5.year_month_number")
            ),
            "left",
        )
        .select(
            col("current_month_period_npi_otl_df_brand.*"),
            coalesce(
                col(
                    "previous_month_period_npi_otl_df_brand.previous_month_period_npi_brand"
                ),
                lit("0"),
            ).alias("previous_month_period_npi_brand"),
            coalesce(col("previous2.previous_month_period_npi_brand_2"), lit("0")).alias(
                "previous2"
            ),
            coalesce(col("previous3.previous_month_period_npi_brand_3"), lit("0")).alias(
                "previous3"
            ),
            coalesce(col("previous4.previous_month_period_npi_brand_4"), lit("0")).alias(
                "previous4"
            ),
            coalesce(col("previous5.previous_month_period_npi_brand_5"), lit("0")).alias(
                "previous5"
            ),
        )
        .distinct()
    )

    # COMMAND ----------

    # DBTITLE 1,Brand Monthly Insert on c+x
    npi_otl_monthly_aggregation_df_brand = DeltaTable.forName(
        spark,
        f"{database_name}.{SystemConfig.tables_list['npi_otl_brand_monthly_aggregation']}",
    )
    fiscal_calendar_month_start_date_df = (
        Read()
        .table("l2", "jnj_calendar")
        .filter(col("calendar_date") == current_date())
        .select(
            when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
            .otherwise(0)
            .alias("run_flag")
        )
        .first()["run_flag"]
    )
    if fiscal_calendar_month_start_date_df == 1:
        npi_otl_monthly_aggregation_df_brand.delete(
            "kelvin_brand_npi_fiscal_year_month = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
        )
        npi_otl_monthly_aggregation_insert_df_brand = npi_month_wise_df_brand.filter(
            (
                col("kelvin_brand_npi_fiscal_year_month")
                == date_format(
                    to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
                )
            )
        )
        Utils.insertintoTable(
            npi_otl_monthly_aggregation_insert_df_brand,
            env,
            SystemConfig.tables_list["npi_otl_brand_monthly_aggregation"],
        )
    else:
        print("Stop")

    # COMMAND ----------

    # DBTITLE 1,YTD BRAND When Aggregration Is Null
    ### otldc_ww
    current_month_period_npi_otldc_df_brand_ytd = (
        kelvin_npi_automation_reported_brand_is_null_ww.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyy"),
        )
        .groupBy(
            col("product_availability_plan_date"),
            col("Kelvin_reported_brand"),
            col("aggregation"),
        )
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_availability_plan_date").alias("kelvin_brand_npi_fiscal_year"),
            when(col("aggregation").isNull() | (col("aggregation") == lit("")), lit("null"))
            .otherwise(col("aggregation"))
            .alias("kelvin_brand_npi_aggregation_ytd"),
            concat(
                upper(trim(col("Kelvin_reported_brand"))),
                lit("-SERVICE-ON-TIME LAUNCH-OTL (DC)"),
            ).alias("kelvin_brand_npi_ytd_metric_uid"),
            upper(trim(col("Kelvin_reported_brand"))).alias("Kelvin_reported_brand_npi"),
        )
        .distinct()
    )


    ###### otl_dc_g15
    current_month_period_npi_otlg15_df_brand_ytd = (
        kelvin_npi_automation_reported_brand_g15_isnull.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyy"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(
            col("product_first_sales_plan_date"),
            col("Kelvin_reported_brand"),
            col("aggregation"),
        )
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_first_sales_plan_date").alias("kelvin_brand_npi_fiscal_year"),
            when(col("aggregation").isNull() | (col("aggregation") == lit("")), lit("null"))
            .otherwise(col("aggregation"))
            .alias("kelvin_brand_npi_aggregation_ytd"),
            concat(
                upper(trim(col("Kelvin_reported_brand"))),
                lit("-SERVICE-ON-TIME LAUNCH-OTL (G15)"),
            ).alias("kelvin_brand_npi_ytd_metric_uid"),
            upper(trim(col("Kelvin_reported_brand"))).alias(
                "Kelvin_reported_brand_npi_ytd"
            ),
        )
        .distinct()
    )


    # COMMAND ----------

    # DBTITLE 1,BRAND YTD When aggregration is not null otldc_ww
    ### otldc_ww
    current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd = (
        kelvin_npi_automation_aggregation_brand_isnot_null_ww.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyy"),
        )
        .groupBy(col("product_availability_plan_date"), col("aggregation"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_availability_plan_date").alias("kelvin_brand_npi_fiscal_year"),
            col("aggregation").alias("kelvin_brand_npi_aggregation_ytd"),
            lit("-SERVICE-ON-TIME LAUNCH-OTL (DC)").alias(
                "kelvin_brand_npi_ytd_metric_uid"
            ),
        )
    )

    join_kelvin_current_npi_otldc_automation_aggregation_yearly_df = (
        current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.alias(
            "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd"
        )
        .join(
            kelvin_brand_npi_otl_automation_df_ww.alias(
                "kelvin_brand_npi_otl_automation_df_ww"
            ),
            (
                col("kelvin_brand_npi_otl_automation_df_ww.aggregation")
                == col(
                    "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_aggregation_ytd"
                )
            )
            & (
                date_format(
                    col(
                        "kelvin_brand_npi_otl_automation_df_ww.product_availability_plan_date"
                    ),
                    "yyyy",
                )
                == (
                    col(
                        "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_fiscal_year"
                    )
                )
            ),
            "left",
        )
        .select(
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_on_time_launch"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_total_launches"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_fiscal_year"
            ),
            col(
                "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_aggregation_ytd"
            ),
            concat(
                col("kelvin_brand_npi_otl_automation_df_ww.Kelvin_reported_brand"),
                col(
                    "current_month_period_npi_otldc_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_ytd_metric_uid"
                ),
            ).alias("kelvin_brand_npi_ytd_metric_uid"),
            trim(col("kelvin_brand_npi_otl_automation_df_ww.Kelvin_reported_brand")).alias(
                "Kelvin_reported_brand_npi_ytd"
            ),
        )
        .distinct()
    )

    # COMMAND ----------

    # DBTITLE 1,BRAND YTD When aggregration is not null otl_dc_g15
    ###### otl_dc_g15
    current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd = (
        kelvin_npi_automation_aggregation_brand_g15_notnull.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyy"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(col("product_first_sales_plan_date"), col("aggregation"))
        .agg(
            sum(
                when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0)
            ).alias("on_time_launch_count"),
            count("*").alias("kelvin_brand_npi_total_launches"),
        )
        .select(
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("kelvin_brand_npi_on_time_launch"),
            col("kelvin_brand_npi_total_launches"),
            col("product_first_sales_plan_date").alias("kelvin_brand_npi_fiscal_year"),
            col("aggregation").alias("kelvin_brand_npi_aggregation_ytd"),
            lit("-SERVICE-ON-TIME LAUNCH-OTL (G15)").alias(
                "kelvin_brand_npi_monthly_metric_uid"
            ),
        )
    )

    join_kelvin_current_npi_otlg15_automation_aggregation_yearly_df = (
        current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.alias(
            "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd"
        )
        .join(
            kelvin_brand_npi_otl_g15_automation_df.alias(
                "kelvin_brand_npi_otl_g15_automation_df"
            ),
            (
                col("kelvin_brand_npi_otl_g15_automation_df.aggregation")
                == col(
                    "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_aggregation_ytd"
                )
            )
            & (
                date_format(
                    col(
                        "kelvin_brand_npi_otl_g15_automation_df.product_first_sales_plan_date"
                    ),
                    "yyyy",
                )
                == (
                    col(
                        "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_fiscal_year"
                    )
                )
            ),
            "left",
        )
        .select(
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_on_time_launch"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_total_launches"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_fiscal_year"
            ),
            col(
                "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_aggregation_ytd"
            ),
            concat(
                col("kelvin_brand_npi_otl_g15_automation_df.Kelvin_reported_brand"),
                col(
                    "current_month_period_npi_otlg15_df_brand_aggregration_not_null_ytd.kelvin_brand_npi_monthly_metric_uid"
                ),
            ).alias("kelvin_brand_npi_monthly_metric_uid"),
            trim(col("kelvin_brand_npi_otl_g15_automation_df.Kelvin_reported_brand")).alias(
                "Kelvin_reported_brand_npi"
            ),
        )
        .distinct()
    )

    # COMMAND ----------

    # DBTITLE 1,Brand is null union all YTD data frame
    current_month_period_npi_otl_union_brand_yearly = (
        current_month_period_npi_otldc_df_brand_ytd.union(
            current_month_period_npi_otlg15_df_brand_ytd
        )
        .union(join_kelvin_current_npi_otlg15_automation_aggregation_yearly_df)
        .union(join_kelvin_current_npi_otldc_automation_aggregation_yearly_df)
    ).distinct()

    # COMMAND ----------

    # DBTITLE 1,Created the dataframe with required columns
    current_yearly_period_npi_otl_df_brand = (
        current_month_period_npi_otl_union_brand_yearly.groupBy(
            col("kelvin_brand_npi_fiscal_year"),
            col("kelvin_brand_npi_ytd_metric_uid"),
            col("kelvin_brand_npi_aggregation_ytd"),
            col("Kelvin_reported_brand_npi"),
        )
        .agg(
            (
                sum(col("kelvin_brand_npi_on_time_launch"))
                / sum(col("kelvin_brand_npi_total_launches"))
                * 100
            )
            .cast("decimal(13,1)")
            .alias("current_period_npi_brand_yearly")
        )
        .select(
            current_date().alias("snapshot_date"),
            col("current_period_npi_brand_yearly"),
            when(
                col("kelvin_brand_npi_aggregation_ytd").isNull()
                | (col("kelvin_brand_npi_aggregation_ytd") == lit("")),
                lit("null"),
            )
            .otherwise(col("kelvin_brand_npi_aggregation_ytd"))
            .alias("kelvin_brand_npi_aggregation_yearly"),
            col("kelvin_brand_npi_ytd_metric_uid").alias(
                "kelvin_brand_npi_metric_uid_yearly"
            ),
            col("kelvin_brand_npi_fiscal_year"),
            col("Kelvin_reported_brand_npi").alias("Kelvin_reported_brand_npi_yearly"),
            date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            ).alias("year_month_number_npi"),
        )
        .where(
            col("kelvin_brand_npi_fiscal_year")
            == date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyy"
            )
        )
        .distinct()
    )

    # COMMAND ----------

    # DBTITLE 1,Brand YTD insert logic C+X
    npi_otl_yearly_aggregation_df_brand = DeltaTable.forName(
        spark,
        f"{database_name}.{SystemConfig.tables_list['npi_otl_brand_yearly_aggregation']}",
    )
    fiscal_calendar_month_start_date_df = (
        Read()
        .table("l2", "jnj_calendar")
        .filter(col("calendar_date") == current_date())
        .select(
            when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
            .otherwise(0)
            .alias("run_flag")
        )
        .first()["run_flag"]
    )
    if fiscal_calendar_month_start_date_df == 1:
        npi_otl_yearly_aggregation_df_brand.delete(
            "year_month_number_npi = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
        )
        npi_otl_yearly_aggregation_insert_df_brand = (
            current_yearly_period_npi_otl_df_brand.filter(
                (
                    col("year_month_number_npi")
                    == date_format(
                        to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
                    )
                )
            )
        )
        Utils.insertintoTable(
            npi_otl_yearly_aggregation_insert_df_brand,
            env,
            SystemConfig.tables_list["npi_otl_brand_yearly_aggregation"],
        )
    else:
        print("Stop")

    # COMMAND ----------

    # MAGIC %md
    # MAGIC                                                           HIERARCHY AUTOMATION

    # COMMAND ----------

    kelvin_npi_otl_df = spark.table(
        f"{database_name}.{SystemConfig.tables_list['npi_otl']}"
    )
    kelvin_npi_otl_automation_df = kelvin_npi_otl_df.filter(
        (
            date_format(col("product_availability_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
        & (col("original_record") == 1)
    )  ###### OTL DC WW
    kelvin_npi_otl_g15_automation_df = kelvin_npi_otl_df.filter(
        (
            date_format(col("product_first_sales_plan_date"), "yyyyMM")
            <= date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            )
        )
        & (col("original_record") == 1)
    )
    ###### OTL DC G15

    # COMMAND ----------

    ### otldc_ww
    hierarchy_current_month_period_npi_otldc_df = (
        kelvin_npi_otl_automation_df.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyyMM"),
        )
        .groupBy(
            col("product_availability_plan_date"), col("country_code"), col("country_name")
        )
        .agg(
            (
                sum(
                    when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
                )
                * lit(100)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            "country_code",
            "country_name",
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_availability_plan_date").alias("fiscal_year_month_otl_dc"),
            lit("On-Time Launch (DC)").alias("kpi_name"),
            lit("").alias("item_name"),
            lit("Green: >= target").alias("target_interpretation"),
            lit("%").alias("uom"),
            lit("month").alias("frequency"),
        )
    )
    ###### otl_dc_g15
    hierarchy_current_month_period_npi_otlg15_df = (
        kelvin_npi_otl_g15_automation_df.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyyMM"),
        )
        .filter((col("country_g15_code") == lit("Yes")))
        .groupBy(
            col("product_first_sales_plan_date"), col("country_code"), col("country_name")
        )
        .agg(
            (
                sum(when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0))
                * lit(100)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            "country_code",
            "country_name",
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_first_sales_plan_date").alias("fiscal_year_month_otl_dc"),
            lit("On-Time Launch (G15)").alias("kpi_name"),
            lit("").alias("item_name"),
            lit("Green: >= target").alias("target_interpretation"),
            lit("%").alias("uom"),
            lit("month").alias("frequency"),
        )
    )


    # COMMAND ----------

    hierarchy_current_month_period_npi_otl_union = (
        hierarchy_current_month_period_npi_otldc_df.unionByName(
            hierarchy_current_month_period_npi_otlg15_df
        )
    )
    hierarchy_current_month_period_npi_otl_union = (
        hierarchy_current_month_period_npi_otl_union.withColumn(
            "snapshot_date", current_date()
        ).withColumn(
            "current_month_percentage",
            (col("on_time_launch") / col("total_launches")).cast("decimal(13,2)"),
        )
    )

    # COMMAND ----------

    hierarchy_npi_otl_monthly_aggregation_df = DeltaTable.forName(
        spark,
        f"{database_name}.{SystemConfig.tables_list['hierarchy_npi_otl_monthly_aggregation']}",
    )
    fiscal_calendar_month_start_date_df = (
        Read()
        .table("l2", "jnj_calendar")
        .filter(col("calendar_date") == current_date())
        .select(
            when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
            .otherwise(0)
            .alias("run_flag")
        )
        .first()["run_flag"]
    )
    if fiscal_calendar_month_start_date_df == 1:
        hierarchy_npi_otl_monthly_aggregation_df.delete(
            "fiscal_year_month_otl_dc = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
        )
        hierarchy_npi_otl_monthly_aggregation_insert_df = (
            hierarchy_current_month_period_npi_otl_union.filter(
                (
                    col("fiscal_year_month_otl_dc")
                    == date_format(
                        to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
                    )
                )
            )
        )
        Utils.insertintoTable(
            hierarchy_npi_otl_monthly_aggregation_insert_df,
            env,
            SystemConfig.tables_list["hierarchy_npi_otl_monthly_aggregation"],
        )
    else:
        print("Stop")

    # COMMAND ----------

    ###### otldc_ww
    hierarchy_actual_ytd_period_npi_otldc_df = (
        kelvin_npi_otl_automation_df.withColumn(
            "product_availability_plan_date",
            date_format(col("product_availability_plan_date"), "yyyy"),
        )
        .filter(
            year(col("product_availability_plan_date"))
            == year(add_months(current_date(), -1))
        )
        .groupBy(
            col("product_availability_plan_date"), col("country_code"), col("country_name")
        )
        .agg(
            (
                sum(
                    when(upper(trim(col("on_time_launch_dc"))) == "ON-TIME", 1).otherwise(0)
                )
                * lit(100)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            "country_code",
            "country_name",
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_availability_plan_date").alias("fiscal_year_otl"),
            lit("On-Time Launch (DC)").alias("kpi_name"),
            lit("").alias("item_name"),
            lit("Green: >= target").alias("target_interpretation"),
            lit("%").alias("uom"),
            lit("month").alias("frequency"),
        )
    )
    ###### otldc_g15
    hierarchy_actual_ytd_period_npi_otlg15_df = (
        kelvin_npi_otl_g15_automation_df.withColumn(
            "product_first_sales_plan_date",
            date_format(col("product_first_sales_plan_date"), "yyyy"),
        )
        .filter(
            (col("country_g15_code") == lit("Yes"))
            & (
                year(col("product_first_sales_plan_date"))
                == year(add_months(current_date(), -1))
            )
        )
        .groupBy(
            col("product_first_sales_plan_date"), col("country_code"), col("country_name")
        )
        .agg(
            (
                sum(when(upper(trim(col("on_time_launch"))) == "ON-TIME", 1).otherwise(0))
                * lit(100)
            ).alias("on_time_launch_count"),
            count("*").alias("total_launches"),
        )
        .select(
            "country_code",
            "country_name",
            coalesce(col("on_time_launch_count"), lit("0"))
            .cast("decimal(13)")
            .alias("on_time_launch"),
            col("total_launches"),
            col("product_first_sales_plan_date").alias("fiscal_year_otl"),
            lit("On-Time Launch (G15)").alias("kpi_name"),
            lit("").alias("item_name"),
            lit("Green: >= target").alias("target_interpretation"),
            lit("%").alias("uom"),
            lit("month").alias("frequency"),
        )
    )


    # COMMAND ----------

    hierarchy_actual_ytd_period_npi_otl_union = (
        hierarchy_actual_ytd_period_npi_otldc_df.unionByName(
            hierarchy_actual_ytd_period_npi_otlg15_df
        )
    )
    hierarchy_actual_ytd_period_npi_otl_union = (
        hierarchy_actual_ytd_period_npi_otl_union.withColumn(
            "snapshot_date", current_date()
        )
        .withColumn(
            "actual_ytd_percentage",
            (col("on_time_launch") / col("total_launches")).cast("decimal(13,2)"),
        )
        .withColumn(
            "year_month_number",
            date_format(
                to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
            ),
        )
    )

    # COMMAND ----------

    # hierarchy_npi_otl_yearly_aggregation_df = DeltaTable.forName(
    #     spark,
    #     f"{database_name}.{SystemConfig.tables_list['hierarchy_npi_otl_yearly_aggregation']}",
    # )
    # fiscal_calendar_month_start_date_df = (
    #     Read()
    #     .table("l2", "jnj_calendar")
    #     .filter(col("calendar_date") == current_date())
    #     .select(
    #         when(current_date() == date_add(col("calendar_month_start_date"), 10), 1)
    #         .otherwise(0)
    #         .alias("run_flag")
    #     )
    #     .first()["run_flag"]
    # )
    # if fiscal_calendar_month_start_date_df == 1:
    #     hierarchy_npi_otl_yearly_aggregation_df.delete(
    #         "year_month_number = date_format(to_timestamp(add_months(current_date(),-1),'yyyy-MM-dd'),'yyyyMM')"
    #     )
    #     hierarchy_npi_otl_yearly_aggregation_insert_df = (
    #         hierarchy_actual_ytd_period_npi_otl_union.filter(
    #             (
    #                 col("year_month_number")
    #                 == date_format(
    #                     to_timestamp(add_months(current_date(), -1), "yyyy-MM-dd"), "yyyyMM"
    #                 )
    #             )
    #             & col("fiscal_year_otl").isNotNull()
    #         )
    #     )
    #     Utils.insertintoTable(
    #         hierarchy_npi_otl_yearly_aggregation_insert_df,
    #         env,
    #         SystemConfig.tables_list["hierarchy_npi_otl_yearly_aggregation"],
    #     )
    # else:
    #     print("Stop")