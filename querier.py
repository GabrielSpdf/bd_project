def cartesian_product(from_):
    if len(from_) == 1:
        return from_[0]
    elif len(from_) == 2:
        return from_[0].merge(from_[1], how = 'cross')
    else:
        return cartesian_product(from_[0], from_[1:])

def break_into_conditions(where):
    result = []
    if len(where) == 0:
        return result
    buf = []
    for word in where:
        if word != 'and':
            buf.append(word)
        else:
            result.append(buf)
            buf = []
    result.append(buf)
    return result

def query(select, from_, where):
    from_big_table = cartesian_product(from_)
    if select[0] == '*':
        select = from_big_table.columns
    where_conditions = break_into_conditions(where)

    result = from_big_table
    for condition in where_conditions:
        if condition[1] == '=':
            try:
                result = result.loc[result[condition[0]] == result[condition[2]]]
            except:
                result = result.loc[result[condition[0]] == condition[2]]
        elif condition[1] == '!=':
            try:
                result = result.loc[result[condition[0]] != result[condition[2]]]
            except:
                result = result.loc[result[condition[0]] != condition[2]]
        elif condition[1] == '>':
            try:
                result = result.loc[result[condition[0]] > result[condition[2]]]
            except:
                result = result.loc[result[condition[0]] > float(condition[2])]
        elif condition[1] == '<':
            try:
                result = result.loc[result[condition[0]] < result[condition[2]]]
            except:
                result = result.loc[result[condition[0]] < float(condition[2])]
    result = result.filter(items = select)
    print(result)