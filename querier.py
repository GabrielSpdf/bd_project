def cartesian_product(from_):
    if len(from_) == 2:
        table1 = from_[0].split('\n')
        table2 = from_[1].split('\n')
        cp = []
        for line1 in table1:
            for line2 in table2:
                cp.append(line1+';'+line2)
        return cp
    else:
        return cartesian_product(from_[0], from_[1:])

def query(select, from_, where, db):
    from_big_table = cartesian_product(from_)
    print(from_big_table)