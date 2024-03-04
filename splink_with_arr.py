import duckdb
import splink.duckdb.comparison_level_library as cll
import splink.duckdb.comparison_library as cl
from splink.duckdb.blocking_rule_library import block_on
from splink.duckdb.linker import DuckDBLinker

from sql_fns import add_arr_of_rel_frequencies, get_product_of_rel_token_freq

path = "./data/BasicCompanyData-2024-03-01-part1_7.csv"


sql = f"""
select
    CompanyName as company_name,
    "RegAddress.AddressLine1" as address_line_1,
    "RegAddress.PostTown" as post_town,
    "RegAddress.PostCode" as postcode,
    "SICCode.SicText_1" as sic_code_1,
    "CompanyCategory" as company_category,
from read_csv('{path}')
limit 10000
"""
in_df = duckdb.sql(sql)

in_df_with_arr = add_arr_of_rel_frequencies(in_df, "company_name", "company_name_arr")

in_df_with_arr = in_df_with_arr.reset_index().rename(columns={"index": "unique_id"})
in_df_with_arr

company_name = {
    "output_column_name": "company_name",
    "comparison_levels": [
        cll.null_level("company_name"),
        cll.exact_match_level("company_name"),
        {
            "sql_condition": f"{get_product_of_rel_token_freq('company_name_arr')} < 0.00000001",
            "label_for_charts": "< 0.000001",
        },
        {
            "sql_condition": f"{get_product_of_rel_token_freq('company_name_arr')} < 0.000001",
            "label_for_charts": "> 0.00001",
        },
        {
            "sql_condition": f"{get_product_of_rel_token_freq('company_name_arr')} < 0.0001",
            "label_for_charts": "> 0.0001",
        },
        {
            "sql_condition": f"{get_product_of_rel_token_freq('company_name_arr')} < 0.001",
            "label_for_charts": "> 0.001",
        },
        {
            "sql_condition": f"{get_product_of_rel_token_freq('company_name_arr')} < 0.1",
            "label_for_charts": "> 0.1",
        },
        {"sql_condition": "ELSE", "label_for_charts": "All other comparisons"},
    ],
    "comparison_description": "Exact match vs. Col within levenshtein thresholds 1, 2 vs. anything else",
}

settings = {
    "link_type": "dedupe_only",
    "comparisons": [
        company_name,
        cl.levenshtein_at_thresholds("address_line_1", 3),
        cl.levenshtein_at_thresholds("postcode", 1),
        cl.exact_match("sic_code_1"),
        cl.exact_match("company_category"),
    ],
    "blocking_rules_to_generate_predictions": [block_on("postcode")],
}

linker = DuckDBLinker(in_df_with_arr, settings)

linker.estimate_u_using_random_sampling(max_pairs=1e6)

linker.match_weights_chart()
linker.m_u_parameters_chart()

linker.predict(threshold_match_probability=0.5).as_pandas_dataframe(limit=100).sample(
    10
)
