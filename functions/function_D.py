import pymongo
import datetime

def xact_D(database, xact):
    t1 = datetime.datetime.now()
    print("========== DELIVERY TRANSACTION ==========")

    line = xact.rstrip().split(',')
    w_id = int(line[1])
    carrier_id = int(line[2])

    # iter through every district under the warehouse
    for i in range(10):
        o_id = None
        d_id = i + 1
        # get the oldest un-delivered order if exists (equal to smallest o_id)
        result = database.collection3.find({"w_id": w_id, "d_id": d_id, "o_carrier_id": 0}, {"o_id": 1, "c_id": 1, "order_lines": 1, "_id": 0}).sort("o_id", 1).limit(1)
        for row in result:
            o_id = row["o_id"]
            c_id = row["c_id"]
            order_lines = row["order_lines"]

        if o_id:
            # update delivery time and calculate total order-line amount
            ol_amount = 0
            ts = str(datetime.datetime.now())
            for i in range(len(order_lines)):
                order_lines[i]["ol_delivery_d"] = ts
                ol_amount += order_lines[i]["ol_amount"]

            # update order and order_lines
            result = database.collection3.update({"w_id": w_id, "d_id": d_id, "o_id": o_id}, {"$set": {"o_carrier_id": carrier_id, "order_lines": order_lines}})

            # update customer info
            result = database.collection2.update({"w_id": w_id, "d_id": d_id, "c_id": c_id}, {"$inc": {"c_balance": ol_amount, "c_delivery_cnt": 1}})

    print()

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'D', latency
