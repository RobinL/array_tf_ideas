import duckdb


def add_arr_of_rel_frequencies(in_duckdb_df, in_name, out_col_name):

    sql = f"""
    select
        {in_name},
        trim(regexp_replace({in_name}, '[^a-zA-Z ]', '', 'g'))
            as {in_name}_clean
    from in_duckdb_df

    """
    clean_text = duckdb.sql(sql)

    sql = f"""
    select
        {in_name},
        string_split({in_name}_clean, ' ') as {in_name}_tokenised
    from clean_text

    """
    tokenised = duckdb.sql(sql)

    sql = f"""
    select
        token,
        count(*)  / sum(count(*)) over() as relative_frequency
    from (
        select
            unnest({in_name}_tokenised) as token
        from tokenised
    )
    group by token
    order by relative_frequency desc
    """
    token_counts = duckdb.sql(sql)

    sql = f"""
    with
    tokens_exploded as (
    select
        {in_name}, unnest({in_name}_tokenised) as token
    from tokenised)

    select tokens_exploded.*, token_counts.relative_frequency
    from tokens_exploded
    left join token_counts
    on tokens_exploded.token = token_counts.token

    """
    with_token_relative_frequency = duckdb.sql(sql)

    sql = f"""
    select
        {in_name},
        list(struct_pack(token := token, relative_frequency := relative_frequency))
            as token_relative_frequency_arr
    from with_token_relative_frequency
    group by {in_name}
    """
    in_name_with_arr_of_freq = duckdb.sql(sql)

    sql = f"""
    select in_duckdb_df.*,
    in_name_with_arr_of_freq.token_relative_frequency_arr as {out_col_name}
    from in_duckdb_df
    left join in_name_with_arr_of_freq
    on in_duckdb_df.{in_name} = in_name_with_arr_of_freq.{in_name}
    """

    return duckdb.sql(sql).df()


def get_product_of_rel_token_freq(arr_col_name):

    overlapping_tokens_arr = f"""
    array_intersect(
                    list_transform({arr_col_name}_l, x->x.token),
                    list_transform({arr_col_name}_r, x->x.token)
            )"""

    # Cant use array intersect on struct, hence this workaround
    filter_tf_for_intersection = f"""
    array_filter({arr_col_name}_l,
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
    return product_of_relative_frequencies
