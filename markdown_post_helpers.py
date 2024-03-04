import duckdb

path_1 = "./data/BasicCompanyData-2024-03-01-part1_7.csv"
path_2 = "./data/two_rows.csv"


sql = f"""
with ch as (
select *
from read_csv('{path_1}')
limit 10000
),
two_rows as (
select *, '2' as my_order
from read_csv('{path_2}')
),
unordered as (
select *, '1' as my_order from ch
UNION ALL
select * from two_rows
)
select * from unordered
order by my_order desc
"""
in_df = duckdb.sql(sql)


# The CompanyName column contains the name of the company
# We want to:
# - 1. clean the data of punctuation and special characters, ensure uppercase
# - 1. tokenise it into an array of words
# - 3. count the frequency of each word

# Begin by doing 1. clean the data of punctuation and special characters, ensure uppercase


sql = """
select
    CompanyName,
    trim(regexp_replace(CompanyName, '[^a-zA-Z ]', '', 'g')) as company_name_clean
from in_df

"""
company_name_clean = duckdb.sql(sql)
company_name_clean
# Next, we want to tokenise the company_name_clean column into an array of words

sql = """
select
    CompanyName,
    string_split(company_name_clean, ' ') as company_name_tokenised
from company_name_clean

"""
company_name_tokenised = duckdb.sql(sql)
company_name_tokenised

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
company_with_token_relative_frequency

sql = """
select
    CompanyName,
    list(struct_pack(token := token, relative_frequency := relative_frequency)) as token_relative_frequency_arr
from company_with_token_relative_frequency
group by CompanyName
"""
companyname_with_arr_of_freq = duckdb.sql(sql)


sql = """
select *
from
companyname_with_arr_of_freq
WHERE CompanyName LIKE '%POSEIPORT%'
"""
# pd.options.display.max_colwidth = 10000


marina_with_tf = duckdb.sql(sql)


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


import sqlglot

s = sqlglot.parse_one(
    """
                  list_reduce(
        list_prepend(1.0,
            list_transform(
    array_filter(company_name_arr_l,
    y -> array_contains(
    array_intersect(
                    list_transform(company_name_arr_l, x->x.token),
                    list_transform(company_name_arr_r, x->x.token)
            ), y.token))
    , x -> x.relative_frequency)
        ),
    (p,q) -> p*q)
     < 0.000001"""
).sql(dialect="duckdb", pretty=True)
print(s)
