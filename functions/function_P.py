import pymongo
import datetime


def xact_P(database, xact):
    t1 = datetime.datetime.now()
    print('========== PAYMENT TRANSACTION ==========')
    line = xact.rstrip().split(',')

    # get the input
    w_id = int(line[1])
    d_id = int(line[2])
    c_id = int(line[3])
    payment = float(line[4])

    # get customer info
    result = database.collection2.find({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"_id": 0})
    for row in result:
        c_balance = row["c_balance"]
        c_city = row["c_city"]
        c_credit = row["c_credit"]
        c_credit_lim = row["c_credit_lim"]
        c_discount = row["c_discount"]
        c_first = row["c_first"]
        c_last = row["c_last"]
        c_middle = row["c_middle"]
        c_phone = row["c_phone"]
        c_since = row["c_since"]
        c_state = row["c_state"]
        c_street_1 = row["c_street_1"]
        c_street_2 = row["c_street_2"]
        c_zip = row["c_zip"]
        d_city = row["d_city"]
        d_state = row["d_state"]
        d_street_1 = row["d_street_1"]
        d_street_2 = row["d_street_2"]
        d_zip = row["d_zip"]
        w_city = row["w_city"]
        w_state = row["w_state"]
        w_street_1 = row["w_street_1"]
        w_street_2 = row["w_street_2"]
        w_zip = row["w_zip"]

    # get warehouse ytd and district ytd
    result = database.collection1.find({"w_id": w_id}, {"w_ytd": 1, "districts.d_ytd": 1, "_id": 0})
    for row in result:
        w_ytd = row["w_ytd"]
        d_ytd = row["districts"][d_id - 1]['d_ytd']

    # update d_ytd and w_ytd
    result = database.collection1.update({"w_id": w_id}, {"$set": {"w_ytd": w_ytd + payment, "districts.{}.d_ytd".format(d_id - 1): d_ytd + payment}})

    # update customer info
    result = database.collection2.update({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"$inc": {"c_balance": -1 * payment, "c_ytd_payment": payment, "c_payment_cnt": 1}})

    # output:
    print("Customer identifier: ({}, {}, {})".format(w_id, d_id, c_id))
    print("Customer name: ({}, {}, {})".format(c_first, c_middle, c_last))
    print("Customer address: ({}, {}, {}, {}, {})".format(c_street_1, c_street_2, c_city, c_state, c_zip))
    print("Customer phone: {}, since: {}, credit: {}, credit limit: {}, discount: {}, balance".format(c_phone, c_since, c_credit, c_credit_lim, round(c_discount, 3), c_balance - payment))
    print("Warehouse address: ({}, {}, {}, {}, {})".format(w_street_1, w_street_2, w_city, w_state, w_zip))
    print("District address: ({}, {}, {}, {}, {})".format(d_street_1, d_street_2, d_city, d_state, d_zip))
    print("Payment: {}".format(payment))

    print()

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'P', latency
