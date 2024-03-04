The main Splink API allows term frequency adjustments to be applied to any column, but the term frequncy adjustments are based on exact matches on the column (see [here](https://github.com/moj-analytical-services/splink/issues/2006#issuecomment-1975101233)).

They are designed to be applied to columns such as first name, so that e.g. `Robin` vs `Robin` gets a higher match weight than `John` vs `John`.

It is harder to conceieve of how term frequency adjustments should work in the case of array based columns, because we're typically looking for array intersections as opposed to exact matches. But we want term frequency adjustment to be based on token frequencies.

## Proposal

A fully working example of the following proposal can be found [here](https://github.com/RobinL/array_tf_ideas/blob/main/splink_with_arr.py). A script that obtains the data and then performs a step by step derivation of the cleaning and array reduction steps can be found [here](https://github.com/RobinL/array_tf_ideas/blob/main/arr_idea.py).

The following outlines the steps:

Consider for example the task of matching company names. We may for example have:

`POSEIPORT MARINA MGT. LIMITED`
vs
`POSEIPORT MARINA MANAGEMENT LTD`

We want the match score to account for the match on the highly unusual token `POSEIPORT`, and the somewhat unusual term `MARINA`. The other tokens are common and less important.

We could clean and tokenise these to an array like:

```
┌─────────────────────────────────┬──────────────────────────────────────┐
│           CompanyName           │        company_name_tokenised        │
│             varchar             │              varchar[]               │
├─────────────────────────────────┼──────────────────────────────────────┤
│ POSEIPORT MARINA MGT. LIMITED   │ [POSEIPORT, MARINA, MGT, LIMITED]    │
│ POSEIPORT MARINA MANAGEMENT LTD │ [POSEIPORT, MARINA, MANAGEMENT, LTD] │
└─────────────────────────────────┴──────────────────────────────────────┘
```

We could then transform the array to include details of term frequencies like:

```
┌─────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│           CompanyName           │                                                                                                                   token_relative_frequency_arr                                                                                                                    │
│             varchar             │                                                                                                        struct(token varchar, relative_frequency double)[]                                                                                                         │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ POSEIPORT MARINA MGT. LIMITED   │ [{'token': POSEIPORT, 'relative_frequency': 6.122199093914534e-05}, {'token': MGT, 'relative_frequency': 3.061099546957267e-05}, {'token': MARINA, 'relative_frequency': 0.00021427696828700869}, {'token': LIMITED, 'relative_frequency': 0.20246112403575364}]  │
│ POSEIPORT MARINA MANAGEMENT LTD │ [{'token': POSEIPORT, 'relative_frequency': 6.122199093914534e-05}, {'token': MANAGEMENT, 'relative_frequency': 0.04717154401861148}, {'token': LTD, 'relative_frequency': 0.09572058283335375}, {'token': MARINA, 'relative_frequency': 0.00021427696828700869}] │
└─────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

```

i.e. `POSEIPORT MARINA MGT. LIMITED `

becomes

```
[
{'token': POSEIPORT, 'relative_frequency': 6.122199093914534e-05},
{'token': MGT, 'relative_frequency': 3.061099546957267e-05},
{'token': MARINA, 'relative_frequency': 0.00021427696828700869},
{'token': LIMITED, 'relative_frequency': 0.20246112403575364}
]
```

## Use in Splink

The above data manipulation would be done as preprocessing steps before bringing the data into Splink.

They ensure that Splink has the raw information it needs to account for token frequencies when comparing the values.

How can we write comparisons which take account of token frequencies?

The following is just an idea - there's probably room for improvement but it does work.

**Step 1:**

Take an array intersect on the `token_relative_frequency_arr` column.

Result:

```
[
{'token': POSEIPORT, 'relative_frequency': 6.122199093914534e-05},
{'token': MARINA, 'relative_frequency': 0.00021427696828700869},
]
```

**Step 2:**

Perform an array reduce, multiplying the `relative_frequency` column:

Calculation: `1 * 6.122199093914534e-05 * 0.00021427696828700869`, where 1 is the starting value for the reduce
Result: `1.311846261093478e-08`

The comparison levels could then be set up as something like:

```
  ├─-- Comparison: CompanyName
    │    ├─-- ComparisonLevel: Exact match on full string with term frequency adjustments
    │    ├─-- ComparisonLevel: array reduction of intersection of token_relative_frequency_arr  < 1e-10
    │    ├─-- ComparisonLevel: array reduction of intersection of token_relative_frequency_arr  < 1e-8
    │    ├─-- ComparisonLevel: array reduction of intersection of token_relative_frequency_arr  < 1e-5
    │    ├─-- ComparisonLevel: all other
```

An example of the full sql for the comparison is:

```
LIST_REDUCE(
  LIST_PREPEND(
    1.0,
    LIST_TRANSFORM(
      FILTER(
        token_relative_frequency_arr_l,
        y -> ARRAY_CONTAINS(
          ARRAY_INTERSECT(
            LIST_TRANSFORM(token_relative_frequency_arr_l, x -> x.token),
            LIST_TRANSFORM(token_relative_frequency_arr_r, x -> x.token)
          ),
          y.token
        )
      ),
      x -> x.relative_frequency
    )
  ),
  (p, q) -> p * q
) < 0.000001
```

A couple of notes on this statement:

- `ARRAY_INTERSECT` does not work on a `struct` so I had to workaround
- `ARRAY_REDUCE` needs a starting value hence `LIST_PREPEND(1.0)`
