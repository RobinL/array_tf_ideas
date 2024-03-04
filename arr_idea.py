import duckdb

# import os
# import zipfile
# import requests
# url = "https://download.companieshouse.gov.uk/BasicCompanyData-2024-03-01-part1_7.zip"
# Download and extract the above zip
# def download_zip(url):
#     r = requests.get(url, stream=True)
#     with open("data.zip", "wb") as file:
#         for chunk in r.iter_content(chunk_size=1024):
#             if chunk:
#                 file.write(chunk)
#     with zipfile.ZipFile("data.zip", "r") as zip_ref:
#         zip_ref.extractall("data")
#     os.remove("data.zip")
#     print("Download and extraction complete")

# download_zip(url)

path = "./data/BasicCompanyData-2024-03-01-part1_7.csv"


sql = f"""
select *
from read_csv('{path}')
limit 100
"""
duckdb.sql(sql).df()


# The CompanyName column contains the name of the company
# We want to:
# - 1. clean the data of punctuation and special characters, ensure uppercase
# - 1. tokenise it into an array of words
# - 3. count the frequency of each word

# Begin by doing 1. clean the data of punctuation and special characters, ensure uppercase


sql = f"""
select
    CompanyName,
    trim(regexp_replace(CompanyName, '[^a-zA-Z ]', '', 'g')) as company_name_clean
from read_csv('{path}')
limit 100
"""
company_name_clean = duckdb.sql(sql)

# Next, we want to tokenise the company_name_clean column into an array of words

sql = """
select
    CompanyName,
    string_split(company_name_clean, ' ') as company_name_tokenised
from company_name_clean
limit 100
"""
company_name_tokenised = duckdb.sql(sql)
company_name_tokenised.df()

# Explode out company_name_tokenised and count the relative frequency of each word
# into a new table called token

sql = """
select
    token,
    count(*)  / sum(count(*)) over() as relative_frequency
from (
    select
        unnest(company_name_tokenised) as token
    from company_name_tokenised
)
group by token
order by relative_frequency desc
"""
token_counts = duckdb.sql(sql)
token_counts.df()

# Use th

sql = """
with
companies_exploded as (
select
    CompanyName, unnest(company_name_tokenised) as token
from company_name_tokenised)

select companies_exploded.*, token_counts.relative_frequency
from companies_exploded
left join token_counts
on companies_exploded.token = token_counts.token

"""
company_with_token_relative_frequency = duckdb.sql(sql)


sql = """
select
    CompanyName,
    list(struct_pack(token := token, relative_frequency := relative_frequency)) as token_relative_frequency_arr
from company_with_token_relative_frequency
group by CompanyName
"""
companyname_with_arr_of_freq = duckdb.sql(sql)
result = companyname_with_arr_of_freq.df()
print(result.loc[1, "CompanyName"])
print(result.loc[1, "token_relative_frequency_arr"])

# Now want a comparison that finds the overlap in two arrays, multiplies the relative frequencies
# and produces a case statement based on the result

# Can then bin token overlaps based on how unusual they ar e

# Image we have data in duckdb sql columns like
token_relative_frequency_arr_l = [
    {"token": "NNOV", "relative_frequency": 0.002717391304347826},
    {"token": "LIMITED", "relative_frequency": 0.125},
]
token_relative_frequency_arr_r = [
    {"token": "NNOV", "relative_frequency": 0.002717391304347826},
    {"token": "INC", "relative_frequency": 0.1},
]

# We want a sql function that returns the overlap of the two arrays based on the token field
sample_of_10 = duckdb.sql("select * from companyname_with_arr_of_freq limit 10")


# can't use array_intersect directly on a struct so need workaround

overlapping_tokens_arr = """
array_intersect(
                list_transform(l.token_relative_frequency_arr, x->x.token),
                list_transform(r.token_relative_frequency_arr, x->x.token)
        )"""

filter_tf_for_intersection = f"""
array_filter(l.token_relative_frequency_arr,
y -> array_contains({overlapping_tokens_arr}, y.token))
"""

# Need to prepend 1.0 because can't reduce empty list
product_of_relative_frequencies = f"""
list_reduce(
    list_prepend(1.0,
        list_transform({filter_tf_for_intersection}, x -> x.relative_frequency)
    ),
(p,q) -> p*q)
"""


sql = f"""
select
l.CompanyName as CompanyName_l,
         r.CompanyName as CompanyName_r,
         l.token_relative_frequency_arr as token_relative_frequency_arr_l,
         r.token_relative_frequency_arr as token_relative_frequency_arr_r,
         {filter_tf_for_intersection} as token_relative_frequency_arr_l,
         {product_of_relative_frequencies} as product_of_relative_frequencies

from sample_of_10 as l
cross join sample_of_10 as r
"""
comparison_with_overlap = duckdb.sql(sql)

comparison_with_overlap
