import pymongo
import datetime


def xact_O(database, xact):
    t1 = datetime.datetime.now()
    print("========== ORDER-STATUS TRANSACTION ==========")

    # get input
    line = xact.rstrip().split(',')
    w_id = int(line[1])
    d_id = int(line[2])
    c_id = int(line[3])

    # get customer info
    result = database.collection2.find({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"_id": 0, "c_first": 1, "c_middle": 1, "c_last": 1, "c_balance": 1})
    for row in result:
        c_first = row["c_first"]
        c_middle = row["c_middle"]
        c_last = row["c_last"]
        c_balance = row["c_balance"]

    # get customer last order -> biggest o_id
    result = database.collection3.find({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"o_id": 1, "o_entry_d": 1, "o_carrier_id": 1, "order_lines": 1, "_id": 0}).sort("o_id", -1).limit(1)
    o_id = None
    for row in result:
        o_id = row["o_id"]
        o_entry_d = row["o_entry_d"]
        o_carrier_id = row["o_carrier_id"]
        order_lines = row["order_lines"]
    if o_id:
        # output:
        print("Customer first name: {}, middle name: {}, last name: {}, balance: {}".format(c_first, c_middle, c_last, round(c_balance, 3)))
        print("Customer last order number: {}, entry date and time: {}, carrier identifier: {}".format(o_id, o_entry_d, o_carrier_id))
        for order_line in order_lines:
            i_id = order_line["ol_i_id"]
            supply_w_id = order_line["ol_supply_w_id"]
            quantity = order_line["ol_quantity"]
            amount = order_line["ol_amount"]
            delivery_d = order_line["ol_delivery_d"]
            print("Item number: {}, supply warehouse number: {}, quantity ordered: {}, total price: {}, delivery time: {}".format(i_id, supply_w_id, quantity, amount, delivery_d))

        print()

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'O', latency
