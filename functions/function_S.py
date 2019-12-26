import pymongo
import datetime


def xact_S(database, xact):
    t1 = datetime.datetime.now()
    print("========== STOCK-LEVEL TRANSACTION ==========")

    # get input
    line = xact.rstrip().split(',')
    w_id = int(line[1])
    d_id = int(line[2])
    t = int(line[3])
    l = int(line[4])
    print("Searching last %d orders in warehouse %d district %d for items with stock < %d" % (l, w_id, d_id, t))

    next_order_no = database.collection1.find_one({"w_id": w_id}, {"districts"})["districts"][d_id - 1]["d_next_o_id"]
    # print(next_order_no)


    # Gather all items from the last l orders
    item_ids = set()
    orders_it = database.collection3.find({"w_id": w_id, "d_id": d_id, "o_id": {"$lt": next_order_no, "$gte": next_order_no - l}}, {"order_lines"})
    # orders_it = database.collection3.find({"w_id": 1, "d_id": 1, "o_id": {"$lt": 3001, "$gte": 3001 - 1}})
    # print(orders_it.count())
    for order in orders_it:
        for order_line in order["order_lines"]:
            item_ids.add(order_line["ol_i_id"])

    # print("%s %s" % (len(item_ids), item_ids))

    # Count the items that did not meet stock level threshold
    num_item = database.collection4.count_documents({"w_id": w_id, "i_id": {"$in": list(item_ids)}, "s_quantity": {"$lt": t}})
    print("Found %s items" % (num_item))

    t2 = datetime.datetime.now()
    latency = (t2 - t1).seconds * 1000 + (t2 - t1).microseconds // 1000
    return 'S', latency
